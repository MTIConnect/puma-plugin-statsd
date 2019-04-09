# coding: utf-8, frozen_string_literal: true

require "json"
require 'socket'
require "puma"
require "puma/plugin"
require "statsd-ruby"


Puma::Plugin.create do

  # uses statsd-ruby client
  class PumaStatsd
    attr_reader :host, :client

    def initialize
      @host = ENV.fetch('APP_STATSD_HOST', '127.0.0.1')
      port = ENV.fetch('APP_STATSD_PORT', 9125)
      statsd_config = [@host, port.to_i]
      @client = Statsd.new(*statsd_config).tap do |client|
        # A small hack to add tags. Bypassing the postfix formatting enforced by the statsd client.
        tags = ENV.fetch('APP_STATSD_TAGS', '')
        client.instance_variable_set(:@postfix, ",#{tags}") if tags != ''
      end
    end

    def enabled?
      !!host
    end
  end

  # Wrap puma's stats in a safe API
  class PumaStats
    def initialize(stats)
      @stats = stats
    end

    def clustered?
      @stats.has_key? "workers"
    end

    def workers
      @stats.fetch("workers", 1)
    end

    def booted_workers
      @stats.fetch("booted_workers", 1)
    end

    def running
      if clustered?
        @stats["worker_status"].map { |s| s["last_status"].fetch("running", 0) }.inject(0, &:+)
      else
        @stats.fetch("running", 0)
      end
    end

    def backlog
      if clustered?
        @stats["worker_status"].map { |s| s["last_status"].fetch("backlog", 0) }.inject(0, &:+)
      else
        @stats.fetch("backlog", 0)
      end
    end

    def pool_capacity
      if clustered?
        @stats["worker_status"].map { |s| s["last_status"].fetch("pool_capacity", 0) }.inject(0, &:+)
      else
        @stats.fetch("pool_capacity", 0)
      end
    end

    def max_threads
      if clustered?
        @stats["worker_status"].map { |s| s["last_status"].fetch("max_threads", 0) }.inject(0, &:+)
      else
        @stats.fetch("max_threads", 0)
      end
    end
  end

  # Puma creates the plugin when encountering `plugin` in the config.
  def initialize(loader)
    @loader = loader
  end

  # We can start doing something when we have a launcher:
  def start(launcher)
    @launcher = launcher

    @statsd = PumaStatsd.new
    if @statsd.enabled?
      @launcher.events.debug "statsd: enabled (host: #{@statsd.host})"
      register_hooks
    else
      @launcher.events.debug "statsd: not enabled (no statsd client configured)"
    end
  end

  private

  def register_hooks
    in_background(&method(:stats_loop))
  end

  def fetch_stats
    JSON.parse(Puma.stats)
  end

  def tags
    tags = {}
    if ENV.has_key?("MY_POD_NAME")
      tags[:pod_name] = ENV.fetch("MY_POD_NAME", "no_pod")
    end
    if ENV.has_key?("STATSD_GROUPING")
      tags[:grouping] = ENV.fetch("STATSD_GROUPING", "no-group")
    end
    tags
  end

  # Send data to statsd every few seconds
  def stats_loop
    sleep 5
    loop do
      @launcher.events.debug "statsd: notify statsd"
      begin
        stats = PumaStats.new(fetch_stats)
        @statsd.client.gauge('puma_workers', stats.workers)
        @statsd.client.gauge('puma_booted_workers', stats.booted_workers)
        @statsd.client.gauge('puma_backlog', stats.backlog)
        @statsd.client.gauge('puma_running', stats.running)
        @statsd.client.gauge('puma_pool_capacity', stats.pool_capacity)
        @statsd.client.gauge('puma_max_threads', stats.max_threads)
      rescue StandardError => e
        @launcher.events.error "! statsd: notify stats failed:\n  #{e.to_s}\n  #{e.backtrace.join("\n    ")}"
      ensure
        sleep 2
      end
    end
  end

end
