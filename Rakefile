require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  
  Jeweler::Tasks.new do |s|
    s.name = "xhr-starling"
    s.version = "0.10.0"
    s.authors = ["Blaine Cook", "Chris Wanstrath", "Britt Selvitelle", "Glenn Rempe", "Abdul-Rahman Advany", "Seth Fitzsimmons", "Harm Aarts", "Chris Gaffney"]
    s.email = ["blaine@twitter.com", "chris@ozmm.org", "abdulrahman@advany.com", "starlingmq@groups.google.com", "harmaarts@gmail.com", "gaffneyc@gmail.com"]
    s.homepage = "http://github.com/xhr/starling/"
    s.summary = "Starling is a lightweight, transactional, distributed queue server"
    s.description = s.summary
    
    s.executables = ["starling", "starling_top"]
    s.require_paths = ["lib"]
    
    s.has_rdoc = true
    s.rdoc_options = ["--quiet", "--title", "starling documentation", "--opname", "index.html", "--line-numbers", "--main", "README.rdoc", "--inline-source"]
    s.extra_rdoc_files = ["README.rdoc", "CHANGELOG", "LICENSE"]
    
    s.add_dependency 'memcache-client', [">= 1.7.0"]
    s.add_dependency 'eventmachine', [">= 0.12.0"]
  end
  
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: sudo gem install jeweler"
end

require 'rake/rdoctask'
Rake::RDocTask.new do |rd|
  rd.main = "README.rdoc"
  rd.rdoc_files.include("README.rdoc", "lib/**/*.rb")
  rd.rdoc_dir = 'doc'
end

# require File.join("lib", "starling", "server")

task :default => :spec

begin
  require 'spec/rake/spectask'
  
  Spec::Rake::SpecTask.new do |t|
    t.ruby_opts = ['-rtest/unit']
    t.spec_files = FileList['spec/*_spec.rb']
    t.fail_on_error = true
  end
rescue LoadError
   puts "RSpec (or a dependency) not available. Install it with: sudo gem install rspec"
end