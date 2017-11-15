require 'set'

require 'ddtrace/context'

module Datadog
  # \ContextFlush is used to cap context size and avoid it using too much memory.
  # It performs memory flushes when required.
  class ContextFlush
    # by default, soft and hard limits are the same
    DEFAULT_MAX_SPANS_BEFORE_PARTIAL_FLUSH = Datadog::Context::DEFAULT_MAX_SPANS
    # by default, never do a partial flush
    DEFAULT_MIN_SPANS_BEFORE_PARTIAL_FLUSH = Datadog::Context::DEFAULT_MAX_SPANS
    # timeout should be lower than the trace agent window
    DEFAULT_PARTIAL_FLUSH_TIMEOUT = 10

    private_constant :DEFAULT_MAX_SPANS_BEFORE_PARTIAL_FLUSH
    private_constant :DEFAULT_MIN_SPANS_BEFORE_PARTIAL_FLUSH
    private_constant :DEFAULT_PARTIAL_FLUSH_TIMEOUT

    def initialize(options = {})
      # max_spans_before_partial_flush is the amount of spans collected before
      # the context starts to partially flush parts of traces. With a setting of 10k,
      # the memory overhead is about 10Mb per thread/context (depends on spans metadata,
      # this is just an order of magnitude).
      @max_spans_before_partial_flush = options.fetch(:max_spans_before_partial_flush,
                                                      DEFAULT_MAX_SPANS_BEFORE_PARTIAL_FLUSH)
      # min_spans_before_partial_flush is the minimum number of spans required
      # for a partial flush to happen on a timeout. This is to prevent partial flush
      # of traces which last a very long time but yet have few spans.
      @min_spans_before_partial_flush = options.fetch(:min_spans_before_partial_flush,
                                                      DEFAULT_MIN_SPANS_BEFORE_PARTIAL_FLUSH)
      # partial_flush_timeout is the limit (in seconds) above which the context
      # considers flushing parts of the trace. Partial flushes should not be done too
      # late else the agent rejects them with a "too far in the past" error.
      @partial_flush_timeout = options.fetch(:partial_flush_timeout,
                                             DEFAULT_PARTIAL_FLUSH_TIMEOUT)
      @partial_traces = []
    end

    # Returns ids of all spans which can be considered as local, partial roots
    # from a partial flush perspective. Also returns the span IDs which have
    # been marked as non flushable, and which should be kept.
    def partial_roots(context)
      # Here it's not totally atomic since current_span could change after it's queried.
      # Worse case: it's held back and not flushed, but it's safe since in that case
      # partial flushing is only "not efficient enought" but never flushes non legit spans.
      current_span = context.current_span
      return nil unless current_span

      marked_ids = ([current_span.span_id] + current_span.parent_ids).to_set
      roots = []
      context.each_span do |span|
        # Skip if span is one of the parents of the current span.
        next if marked_ids.include? span.span_id
        # Skip if the span is not one of the parents of the current span,
        # and its parent is not either. It means it just can't be a local, partial root.
        next unless marked_ids.include? span.parent_id

        roots << span.span_id
      end
      [roots, marked_ids]
    end

    # Return a hash containting all sub traces which are candidates for
    # a partial flush.
    def partial_roots_spans(context)
      roots, marked_ids = partial_roots(context)
      return nil unless roots

      roots_spans = Hash[roots.map { |id| [id, []] }]
      unfinished = Set.new
      context.each_span do |span|
        ids = [span.span_id] + span.parent_ids()
        ids.delete_if { |id| marked_ids.include? id }
        ids.each do |id|
          if roots_spans.include?(id)
            unfinished[id] = true unless span.finished?
            roots_spans[id] << span
          end
        end
      end
      # Do not flush unfinished traces.
      roots_spans.delete_if { |id| unfinished.include? id }
      return nil if roots_spans.empty?
      roots_spans
    end

    def partial_flush(context)
      roots_spans = partial_roots_spans(context)
      return nil unless roots_spans

      traces = []
      flushed_ids = {}
      roots_spans.each_value do |spans|
        next if spans.empty?
        spans.each { |span| flushed_ids[span.span_id] = true }
        traces << spans
      end
      # We need to reject by span ID and not by value, because a span
      # value may be altered (typical example: it's finished by some other thread)
      # since we lock only the context, not all the spans which belong to it.
      context.delete_span_if { |span| flushed_ids.include? span.span_id }
      traces
    end

    # Performs an operation which each partial trace it can get from the context.
    def each_partial_trace(context)
      start_time = context.start_time
      length = context.length
      # Stop and do not flush anything if there are not enough spans.
      return if length < @min_spans_before_partial_flush
      # If there are enough spans, but not too many, check for start time.
      return if length < @max_spans_before_partial_flush &&
                start_time && start_time > Time.now.utc - @partial_flush_timeout
      # Here, either the trace is old or we have too many spans, flush it.
      traces = partial_flush(context)
      return unless traces
      traces.each do |trace|
        yield trace
      end
    end

    private :partial_roots
    private :partial_roots_spans
    private :partial_flush
  end
end
