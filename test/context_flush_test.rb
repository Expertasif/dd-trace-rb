require 'helper'
require 'ddtrace/tracer'
require 'ddtrace/context_flush'

class ContextFlushTest < Minitest::Test
  def test_partial_flush_typical_not_enough_traces
    tracer = get_test_tracer
    context_flush = Datadog::ContextFlush.new
    context = tracer.call_context

    context_flush.each_partial_trace(context) do |_t|
      flunk('nothing should be partially flushed, no spans')
    end

    # the plan:
    #
    # root-------------.
    #   | \______       \
    #   |        \       \
    # child1   child3   child4
    #   |                 |  \_____
    #   |                 |        \
    # child2            child5   child6

    tracer.trace('root') do
      tracer.trace('child1') do
        tracer.trace('child2') do
        end
      end
      tracer.trace('child3') do
        # finished spans are CAPITALIZED
        #
        # root
        #   | \______
        #   |        \
        # CHILD1   child3
        #   |
        #   |
        # CHILD2
        context_flush.each_partial_trace(context) do |t|
          flunk("nothing should be partially flushed, got: #{t}")
        end
      end
      tracer.trace('child4') do
        tracer.trace('child5') do
        end
        tracer.trace('child6') do
        end
      end
      # finished spans are CAPITALIZED
      #
      # root-------------.
      #   | \______       \
      #   |        \       \
      # CHILD1   CHILD3   CHILD4
      #   |                 |  \_____
      #   |                 |        \
      # CHILD2            CHILD5   CHILD6
      context_flush.each_partial_trace(context) do |t|
        flunk("nothing should be partially flushed, got: #{t}")
      end
    end

    context_flush.each_partial_trace(context) do |t|
      flunk("nothing should be partially flushed, got: #{t}")
    end

    assert_equal(0, context.length, 'everything should be written by now')
  end

  def test_partial_flush_typical
    tracer = get_test_tracer
    context_flush = Datadog::ContextFlush.new(min_spans_before_partial_flush: 1,
                                              max_spans_before_partial_flush: 1)
    context = tracer.call_context

    # the plan:
    #
    # root-------------.
    #   | \______       \
    #   |        \       \
    # child1   child3   child4
    #   |                 |  \_____
    #   |                 |        \
    # child2            child5   child6

    action12 = Minitest::Mock.new
    action12.expect(:call_with_names, nil, [%w[child1 child2].to_set])
    action3456 = Minitest::Mock.new
    action3456.expect(:call_with_names, nil, [['child3'].to_set])
    action3456.expect(:call_with_names, nil, [%w[child4 child5 child6].to_set])

    tracer.trace('root') do
      tracer.trace('child1') do
        tracer.trace('child2') do
        end
      end
      tracer.trace('child3') do
        # finished spans are CAPITALIZED
        #
        # root
        #   | \______
        #   |        \
        # CHILD1   child3
        #   |
        #   |
        # CHILD2
        context_flush.each_partial_trace(context) do |t|
          action12.call_with_names(t.map(&:name).to_set)
        end
      end
      tracer.trace('child4') do
        tracer.trace('child5') do
        end
        tracer.trace('child6') do
        end
      end
      # finished spans are CAPITALIZED
      #
      # root-------------.
      #     \______       \
      #            \       \
      #          CHILD3   CHILD4
      #                     |  \_____
      #                     |        \
      #                   CHILD5   CHILD6
      context_flush.each_partial_trace(context) do |t|
        action3456.call_with_names(t.map(&:name).to_set)
      end
    end

    action12.verify
    action3456.verify

    assert_equal(0, context.length, 'everything should be written by now')
  end

  # rubocop:disable Metrics/MethodLength
  def test_partial_flush_mixed
    tracer = get_test_tracer
    context_flush = Datadog::ContextFlush.new(min_spans_before_partial_flush: 1,
                                              max_spans_before_partial_flush: 1)
    context = tracer.call_context

    # the plan:
    #
    # root
    #   | \______
    #   |        \
    # child1   child5
    #   |
    #   |
    # child2
    #   | \______
    #   |        \
    # child3   child6
    #   |        |
    #   |        |
    # child4   child7

    action345 = Minitest::Mock.new
    action345.expect(:call_with_names, nil, [%w[child3 child4].to_set])
    action345.expect(:call_with_names, nil, [%w[child5].to_set])

    root = tracer.start_span('root', child_of: context)
    child1 = tracer.start_span('child1', child_of: root)
    child2 = tracer.start_span('child2', child_of: child1)
    child3 = tracer.start_span('child3', child_of: child2)
    child4 = tracer.start_span('child4', child_of: child3)
    child5 = tracer.start_span('child5', child_of: root)
    child6 = tracer.start_span('child6', child_of: child2)
    child7 = tracer.start_span('child7', child_of: child6)

    context_flush.each_partial_trace(context) do |_t|
      context_flush.each_partial_trace(context) do |_t|
        flunk('nothing should be partially flushed, no span is finished')
      end
    end

    assert_equal(8, context.length)

    [root, child1, child3, child6].each do |span|
      span.finish
      context_flush.each_partial_trace(context) do |t|
        flunk("nothing should be partially flushed, got: #{t}")
      end
    end

    # finished spans are CAPITALIZED
    #
    # ROOT
    #   | \______
    #   |        \
    # CHILD1   child5
    #   |
    #   |
    # child2
    #   | \______
    #   |        \
    # CHILD3   CHILD6
    #   |        |
    #   |        |
    # child4   child7

    child2.finish

    context_flush.each_partial_trace(context) do |t|
      flunk("nothing should be partially flushed, got: #{t}")
    end

    # finished spans are CAPITALIZED
    #
    # ROOT
    #   | \______
    #   |        \
    # CHILD1   child5
    #   |
    #   |
    # CHILD2
    #   | \______
    #   |        \
    # CHILD3   CHILD6
    #   |        |
    #   |        |
    # child4   child7

    child4.finish
    child5.finish

    # finished spans are CAPITALIZED
    #
    # ROOT
    #   | \______
    #   |        \
    # CHILD1   CHILD5
    #   |
    #   |
    # CHILD2
    #   | \______
    #   |        \
    # CHILD3   CHILD6
    #   |        |
    #   |        |
    # CHILD4   child7

    context_flush.each_partial_trace(context) do |t|
      action345.call_with_names(t.map(&:name).to_set)
    end

    child7.finish

    context_flush.each_partial_trace(context) do |t|
      flunk("nothing should be partially flushed, got: #{t}")
    end

    assert_equal(0, context.length, 'everything should be written by now')
  end
end
