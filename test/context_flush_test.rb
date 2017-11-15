require 'helper'
require 'ddtrace/tracer'
require 'ddtrace/context_flush'

module Datadog
  class ContextFlush
    attr_accessor :max_spans_before_partial_flush
    attr_accessor :min_spans_before_partial_flush
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

        context_flush.each_partial_trace(context) do |_t|
          flunk('there should be no trace here, not enough spans to trigger a flush')
        end
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

    context_flush.each_partial_trace(context) do |_t|
      flunk('there should be no trace here, there are no spans at all')
    end
  end

  def test_partial_flush
    tracer = get_test_tracer
    # Trigger early flush.
    context_flush = Datadog::ContextFlush.new(min_spans_before_partial_flush: 1,
                                              max_spans_before_partial_flush: 1)
    context = tracer.call_context

    action12 = Minitest::Mock.new
    action12.expect(:call_with_names, nil, [%w[child1 child2]])
    action3456 = Minitest::Mock.new
    action3456.expect(:call_with_names, nil, [['child3']])
    action3456.expect(:call_with_names, nil, [%w[child4 child5 child6]])

    tracer.trace('root') do
      tracer.trace('child1') do
        tracer.trace('child2') do
        end
      end
      tracer.trace('child3') do
        context_flush.each_partial_trace(context) do |t|
          action12.call_with_names(t.map(&:name))
        end
      end
      tracer.trace('child4') do
        tracer.trace('child5') do
        end
        tracer.trace('child6') do
        end
      end
      context_flush.each_partial_trace(context) do |t|
        action3456.call_with_names(t.map(&:name))
      end
    end

    action12.verify
    action3456.verify

    assert_equal(0, context.length, 'everything should be written by now')
  end
end
