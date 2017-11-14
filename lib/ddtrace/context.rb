require 'thread'

module Datadog
  # \Context is used to keep track of a hierarchy of spans for the current
  # execution flow. During each logical execution, the same \Context is
  # used to represent a single logical trace, even if the trace is built
  # asynchronously.
  #
  # A single code execution may use multiple \Context if part of the execution
  # must not be related to the current tracing. As example, a delayed job may
  # compose a standalone trace instead of being related to the same trace that
  # generates the job itself. On the other hand, if it's part of the same
  # \Context, it will be related to the original trace.
  #
  # This data structure is thread-safe.
  # rubocop:disable Metrics/ClassLength
  class Context
    # DEFAULT_MAX_SPANS_PER_TRACE_SOFT is the amount of spans collected before
    # the context starts to partially flush parts of traces. With a setting of 10k,
    # the memory overhead is about 10Mb per thread/context (depends on spans metadata,
    # this is just an order of magnitude).
    DEFAULT_MAX_SPANS_PER_TRACE_SOFT = 10_000
    # DEFAULT_MAX_SPANS_PER_TRACE_HARD is the amount of spans above which, for a
    # given trace, the context will simply drop and ignore spans, avoiding
    # a high memory usage.
    DEFAULT_MAX_SPANS_PER_TRACE_HARD = 100_000
    # DEFAULT_MIN_SPANS_FOR_FLUSH_TIMEOUT is the minimum number of spans required
    # for a partial flush to happen on a timeout. This is to prevent partial flush
    # of traces which last a very long time but yet have few spans.
    DEFAULT_MIN_SPANS_FOR_FLUSH_TIMEOUT = 100
    # DEFAULT_PARTIAL_FLUSH_TIMEOUT is the limit (in seconds) above which the context
    # considers flushing parts of the trace. Partial flushes should not be done too
    # late else the agent rejects them with a "too far in the past" error.
    DEFAULT_PARTIAL_FLUSH_TIMEOUT = 10

    # Initialize a new thread-safe \Context.
    def initialize(options = {})
      @mutex = Mutex.new
      reset
    end

    def reset(options = {})
      @trace = []
      @parent_trace_id = options.fetch(:trace_id, nil)
      @parent_span_id = options.fetch(:span_id, nil)
      @sampled = options.fetch(:sampled, false)
      @sampling_priority = options.fetch(:sampling_priority, nil)
      @finished_spans = 0
      @current_span = nil
    end

    def trace_id
      @mutex.synchronize do
        @parent_trace_id
      end
    end

    def span_id
      @mutex.synchronize do
        @parent_span_id
      end
    end

    def sampling_priority
      @mutex.synchronize do
        @sampling_priority
      end
    end

    def sampling_priority=(priority)
      @mutex.synchronize do
        @sampling_priority = priority
      end
    end

    # Return the last active span that corresponds to the last inserted
    # item in the trace list. This cannot be considered as the current active
    # span in asynchronous environments, because some spans can be closed
    # earlier while child spans still need to finish their traced execution.
    def current_span
      @mutex.synchronize do
        return @current_span
      end
    end

    def set_current_span(span)
      @current_span = span
      if span
        @parent_trace_id = span.trace_id
        @parent_span_id = span.span_id
        @sampled = span.sampled
      else
        @parent_span_id = nil
      end
    end

    # Add a span to the context trace list, keeping it as the last active span.
    def add_span(span)
      @mutex.synchronize do
        # If hitting the hard limit, just drop spans. This is really a rare case
        # as it means despite the soft limit, the hard limit is reached, so the trace
        # by default has 10000 spans, all of which belong to unfinished parts of a
        # larger trace. This is a catch-all to reduce global memory usage.
        if @max_spans_per_trace_hard > 0 && @trace.length >= (@max_spans_per_trace_hard - 1)
          Datadog::Tracer.log.debug("context full, ignoring span #{span.name}")
          # This span is going to be finished at some point, but will never increase
          # the trace size, so we acknowledge this fact, to avoid to send it to early.
          @finished_spans -= 1
          return
        end
        set_current_span(span)
        @trace << span
        span.context = self
        # If hitting the soft limit, start flushing intermediate data.
        if @max_spans_per_trace_soft > 0 && @trace.length >= @max_spans_per_trace_soft
          Datadog::Tracer.log.debug("context full, span #{span.name} triggers partial flush")
          partial_flush()
        end
      end
    end

    # Mark a span as a finished, increasing the internal counter to prevent
    # cycles inside _trace list.
    def close_span(span)
      @mutex.synchronize do
        @finished_spans += 1
        # Current span is only meaningful for linear tree-like traces,
        # in other cases, this is just broken and one should rely
        # on per-instrumentation code to retrieve handle parent/child relations.
        set_current_span(span.parent)
        return if span.tracer.nil?
        return unless Datadog::Tracer.debug_logging
        if span.parent.nil? && !check_finished_spans
          opened_spans = @trace.length - @finished_spans
          Datadog::Tracer.log.debug("root span #{span.name} closed but has #{opened_spans} unfinished spans:")
          @trace.each do |s|
            Datadog::Tracer.log.debug("unfinished span: #{s}") unless s.finished?
          end
        end
      end
    end

    # Returns if the trace for the current Context is finished or not.
    # Low-level internal function, not thread-safe.
    def check_finished_spans
      @finished_spans > 0 && @trace.length == @finished_spans
    end

    # Returns if the trace for the current Context is finished or not. A \Context
    # is considered finished if all spans in this context are finished.
    def finished?
      @mutex.synchronize do
        return check_finished_spans
      end
    end

    # Returns true if the context is sampled, that is, if it should be kept
    # and sent to the trace agent.
    def sampled?
      @mutex.synchronize do
        return @sampled
      end
    end

    # Returns ids of all spans which can be considered as local, partial roots
    # from a partial flush perspective. Also returns the span IDs which have
    # been marked as non flushable, and which should be kept.
    def partial_roots
      return nil unless @current_span

      marked_ids = Hash[([@current_span.span_id] + @current_span.parent_ids).map { |id| [id, true] }]
      roots = []
      @trace.each do |span|
        # Skip if span is one of the parents of the current span.
        next if marked_ids.key? span.span_id
        # Skip if the span is not one of the parents of the current span,
        # and its parent is not either. It means it just can't be a local, partial root.
        next unless marked_ids.key? span.parent_id

        roots << span.span_id
      end
      [roots, marked_ids]
    end

    # Return a hash containting all sub traces which are candidates for
    # a partial flush.
    def partial_roots_spans
      roots, marked_ids = partial_roots()
      return nil unless roots

      roots_spans = Hash[roots.map { |id| [id, []] }]
      unfinished = {}
      @trace.each do |span|
        ids = [span.span_id] + span.parent_ids()
        ids.reject! { |id| marked_ids.key? id }
        ids.each do |id|
          if roots_spans.key?(id)
            unfinished[id] = true unless span.finished?
            roots_spans[id] << span
          end
        end
      end
      # Do not flush unfinished traces.
      roots_spans.reject! { |id| unfinished.key? id }
      return nil if roots_spans.empty?
      roots_spans
    end

    def partial_flush
      roots_spans = partial_roots_spans()
      return nil unless roots_spans

      flushed_ids = {}
      roots_spans.each_value do |spans|
        next if spans.empty?
        spans.each { |span| flushed_ids[span.span_id] = true }
        @partial_traces << spans
      end
      # We need to reject by span ID and not by value, because a span
      # value may be altered (typical example: it's finished by some other thread)
      # since we lock only the context, not all the spans which belong to it.
      @trace.reject! { |span| flushed_ids.key? span.span_id }
    end

    # Returns both the trace list generated in the current context and
    # if the context is sampled or not. It returns nil, nil if the ``Context`` is
    # not finished. If a trace is returned, the \Context will be reset so that it
    # can be re-used immediately.
    #
    # This operation is thread-safe.
    def get
      @mutex.synchronize do
        trace = @trace
        sampled = @sampled

        # There's a need to flush partial parts of traces when they are getting old:
        # not doing this, partial bits could be flushed alone later, and trigger
        # a "too far in the past" error on the agent.
        # By doing this, we send partial information on the server and take the risk
        # to split a trace which could have been totally in-memory.
        # OTOH the backend will collect these and put them together.
        # Traces which do not have enough spans will not be touched
        # to avoid slicing small things too often.
        unless trace.length <= @min_spans_for_flush_timeout ||
               trace[0].start_time.nil? ||
               trace[0].start_time > Time.now.utc - @partial_flush_timeout
          partial_flush()
        end

        partial_trace = @partial_traces.shift
        return partial_trace, sampled if partial_trace

        return nil, nil unless check_finished_spans()

        reset
        [trace, sampled]
      end
    end

    # Return a string representation of the context.
    def to_s
      @mutex.synchronize do
        # rubocop:disable Metrics/LineLength
        "Context(trace.length:#{@trace.length},sampled:#{@sampled},finished_spans:#{@finished_spans},current_span:#{@current_span})"
      end
    end

    private :reset
    private :check_finished_spans
    private :set_current_span
  end

  # ThreadLocalContext can be used as a tracer global reference to create
  # a different \Context for each thread. In synchronous tracer, this
  # is required to prevent multiple threads sharing the same \Context
  # in different executions.
  class ThreadLocalContext
    # ThreadLocalContext can be used as a tracer global reference to create
    # a different \Context for each thread. In synchronous tracer, this
    # is required to prevent multiple threads sharing the same \Context
    # in different executions.
    def initialize
      self.local = Datadog::Context.new
    end

    # Override the thread-local context with a new context.
    def local=(ctx)
      Thread.current[:datadog_context] = ctx
    end

    # Return the thread-local context.
    def local
      Thread.current[:datadog_context] ||= Datadog::Context.new
    end
  end
end
