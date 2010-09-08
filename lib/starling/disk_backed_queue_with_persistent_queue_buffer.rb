require 'starling/disk_backed_queue'
require 'starling/persistent_queue'

module StarlingServer
  class DiskBackedQueueWithPersistentQueueBuffer
    MAX_PRIMARY_SIZE = 10_000
    def initial_bytes
      @primary.initial_bytes + @backing.initial_bytes
    end

    def total_items
      @primary.total_items + @backing.length
    end

    def current_age
      @primary.current_age
    end

    def logsize
      @primary.logsize
    end

    def backing_logsize
      @backing.logsize
    end

    def length
      @primary.length + @backing.length
    end

    def primary_length
      @primary.length
    end

    def backing_length
      @backing.length
    end

    def initialize(persistence_path, queue_name, allow_out_of_order_reads)
      @preserve_order = !allow_out_of_order_reads
      @primary = PersistentQueue.new(persistence_path, queue_name)
      @backing = DiskBackedQueue.new(persistence_path, queue_name)
    end

    def push(data)
      if should_go_to_backing?
        @backing.push(data)
      else
        @primary.push(data)
      end
    end

    def purge
      @primary.purge
      @backing.purge
    end

    def empty?
      @primary.empty? and @backing.empty?
    end

    def pop
      @backing.consume_log_into(@primary) if @primary.empty?
      @primary.pop
    end
  
    def close
      @primary.close
      @backing.close
    end

    def should_go_to_backing?
      return true if @primary.length >= MAX_PRIMARY_SIZE
      @preserve_order and @backing.any?
    end
  end
end