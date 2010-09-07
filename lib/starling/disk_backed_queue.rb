module StarlingServer
  class DiskBackedQueue
    MAX_LOGFILE_SIZE = 1000

    def initialize(persistence_path, queue_name)
      @persistence_path, @queue_name = persistence_path, queue_name
      @active_log_file_name = latest_log_file
      data = load_log(@active_log_file_name)
      @active_log_file_size = data.size

      if @active_log_file_name.nil?
        @active_log_file_name = new_logfile_name
        FileUtils.mkdir_p(File.dirname(@active_log_file_name))
      end
      @active_log_file = File.open(@active_log_file_name, "ab")
    end

    def length
      (available_log_files.size - 1) * MAX_LOGFILE_SIZE + @active_log_file_size
    end

    def logsize
      available_log_files.inject(0){|sum, f| sum + File.size(f) }
    end

    def push(data)
      Thread.exclusive do
        rotate! if @active_log_file_size >= MAX_LOGFILE_SIZE
        @active_log_file << [data.size].pack("I") + data
        @active_log_file_size += 1
      end
    end

    def consume_log_into(queue)
      Thread.exclusive do
        rotate! if @active_log_file_name == first_log_file
        return unless file = first_log_file
        load_log(file){|i| queue.push(i) }
        File.unlink(file)
      end
    end

    def close
      @active_log_file.close if @active_log_file
      @active_log_file = nil
    end


  protected

    def available_log_files
      Dir[File.join(@persistence_path, "disk_backed_queue", @queue_name, "*")].sort
    end

    def rotate!
      close
      @active_log_file_name = new_logfile_name
      @active_log_file_size = 0
      FileUtils.mkdir_p(File.dirname(@active_log_file_name))
      @active_log_file = File.open(@active_log_file_name, "ab")
    end

    def new_logfile_name(suffix = 0)
      name = File.join(@persistence_path, "disk_backed_queue", @queue_name, "#{Time.now.to_i}-#{suffix}")
      if File.exists?( name )
        new_logfile_name(suffix + 1)
      else
        name
      end
    end

    def latest_log_file
      available_log_files.last
    end

    def first_log_file
      available_log_files.first
    end

    def load_log(file, &block)
      return [] unless file
      data = []
      File.open(file){|f|
        while raw_size = f.read(4)
          next unless raw_size
          size = raw_size.unpack("I").first
          item = f.read(size)
          if block_given?
            yield item
          else
            data << item
          end
        end
      }
      data
    end
  end
end
