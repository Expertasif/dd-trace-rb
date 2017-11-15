require 'helper'
require 'ddtrace/tracer'
require 'ddtrace/context_flush'

module Datadog
  class ContextFlush
    attr_accessor :max_spans_per_trace_soft
    attr_accessor :max_spans_per_trace_hard
    attr_accessor :min_spans_for_flush_timeout
    attr_accessor :partial_flush_timeout

    public :partial_roots
    public :partial_roots_spans
    public :partial_flush
  end
end

class ContextFlushTest < Minitest::Test
  def test_partial_roots_typical
    tracer = get_test_tracer
    context_flush = Datadog::ContextFlush.new
    context = tracer.call_context

    root_id = nil
    child1_id = nil
    child2_id = nil
    child3_id = nil
    tracer.trace('root') do |root|
      root_id = root.span_id
      tracer.trace('child1') do |child1|
        child1_id = child1.span_id
        tracer.trace('child2') do |child2|
          child2_id = child2.span_id
        end
      end
      tracer.trace('child3') do |child3|
        child3_id = child3.span_id

        partial_roots, marked_ids = context_flush.partial_roots(context)
        assert_equal([child1_id], partial_roots)
        assert_equal([root_id, child3_id].to_set, marked_ids)
        partial_roots_spans = context_flush.partial_roots_spans(context)
        assert_includes(partial_roots_spans, child1_id)
      end
    end
  end

  def test_partial_roots_empty
    tracer = get_test_tracer
    context_flush = Datadog::ContextFlush.new
    context = tracer.call_context

    partial_roots, marked_ids = context_flush.partial_roots(context)
    assert_nil(partial_roots)
    assert_nil(marked_ids)
    partial_roots_spans = context_flush.partial_roots_spans(context)
    assert_nil(partial_roots_spans)
  end
end
