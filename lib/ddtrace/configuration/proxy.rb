require 'forwardable'

module Datadog
  class Configuration
    # Proxy provides a hash-like interface for fetching/setting configurations
    class Proxy
      extend Forwardable

      def initialize(integration)
        @integration = integration
      end

      def [](param)
        value = @integration.get_option(param)

        return value.call if value.respond_to?(:call)

        value
      end

      def []=(param, value)
        @integration.set_option(param, value)
      end

      def_delegators :@integration, :to_h, :reset_options!
      def_delegators :to_h, :to_hash, :merge
    end
  end
end
