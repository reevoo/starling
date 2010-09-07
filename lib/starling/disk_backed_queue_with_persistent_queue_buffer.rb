require 'starling/disk_backed_queue'
require 'starling/persistent_queue'

module StarlingServer
  class DiskBackedQueueWithPersistentQueueBuffer
    MAX_PRIMARY_SIZE = 10000
    def initial_bytes
      @primary.initial_bytes
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

    def initialize(persistence_path, queue_name)
      @primary = PersistentQueue.new(persistence_path, queue_name)
      @backing = DiskBackedQueue.new(persistence_path, queue_name)
    end

    def push(data)
      if @primary.length >= MAX_PRIMARY_SIZE or @force_backing
        @force_backing = true
        @backing.push(data)
      else
        @primary.push(data)
      end
    end

    def pop
      if @primary.empty?
        @backing.consume_log_into(@primary)
        @force_backing = false if @primary.empty?
      end
      @primary.pop
    end
  
    def close
      @primary.close
      @backing.close
    end
  end
end