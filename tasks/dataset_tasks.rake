namespace :db do
  namespace :dataset do
    DATASET_DIR = 'db/dataset'
    
    desc "Load a dataset into the current environment's database from #{DATASET_DIR}. Specify DATASET=x to use #{DATASET_DIR}/x instead."
    task :load => 'db:schema:load' do
      require 'ar_fixtures'
      require 'bigdecimal'
      path = dataset_path
      puts "Loading dataset to #{RAILS_ENV}"
      keep_quiet = ENV['DBLOAD_QUIET'].to_s.dup == "true"

      (Dir.glob('app/models/*.rb') + Dir.glob('vendor/plugins/idt_legacy_models/lib/telecom/*.rb')).each { |file| require file }
      models = ActiveRecord::Base.send(:subclasses).select { |ar| ar.to_s != "CGI::Session::ActiveRecordStore::Session" && !ar.to_s.match(/ActiveRecord::.*/) && ar.send(:subclasses).empty? }
      connections = models.inject({}) { |store, model| 
        store[model.table_name.upcase] = model.connection
        store
      }
      
      Dir.glob(path + '/*.yml').each do |fixture_file|
        puts "Loading fixture #{fixture_file.to_s}" unless keep_quiet
        Fixtures.create_fixtures(path, File.basename(fixture_file, '.*')) { connections[File.basename(fixture_file, ".yml").upcase] }
      end
      puts "Done"
    end
    
    desc "Load a single table's dataset, FIXTURE=x, into the current environment's database from #{DATASET_DIR}. Specify DATASET=x to use #{DATASET_DIR}/x instead."
    task :load_fixture => 'db:schema:load' do
      if ENV['FIXTURE']
        require 'ar_fixtures'
        require 'bigdecimal'
        path = dataset_path
        puts "Loading fixture [#{ENV['FIXTURE']}] dataset to #{RAILS_ENV}"
        keep_quiet = ENV['DBLOAD_QUIET'].to_s.dup == "true"
        ActiveRecord::Base.establish_connection(RAILS_ENV.to_sym)
        Dir.glob(path + "/#{ENV['FIXTURE']}.yml").each do |fixture_file|
          puts "Loading fixture #{fixture_file.to_s}" unless keep_quiet
          Fixtures.create_fixtures(path, File.basename(fixture_file, '.*'))
        end
        puts "Done"
      else
        puts "Load a single table's dataset, FIXTURE=x, into the current environment's database from #{DATASET_DIR}. Specify DATASET=x to use #{DATASET_DIR}/x instead."
        puts "... You did not specify which fixture to load."
      end
    end
    
    desc "Dump a dataset from the current environment's database into #{DATASET_DIR}. Specify DATASET=x to use #{DATASET_DIR}/x instead."
    task :dump => :environment do
      require 'ar_fixtures'
      require 'bigdecimal'
      (Dir.glob('app/models/*.rb') + Dir.glob('vendor/plugins/idt_legacy_models/lib/telecom/*.rb')).each { |file| require file } 
      
      path = dataset_path
      FileUtils.mkdir_p path
      
      ActiveRecord::Base.send(:subclasses).select { |ar| ar.to_s != "CGI::Session::ActiveRecordStore::Session" && !ar.to_s.match(/ActiveRecord::.*/) && ar.send(:subclasses).empty? }.each do |ar|
        puts "Now dumping #{ ar.to_s } to #{path}"
        begin
          ar.to_fixture(path, ENV['LIMIT'])
        rescue => e
          puts "Failed: #{e.message}"
        end
      end
      puts "Done"
    end
    
    desc "Cleans up all dataset information in #{DATASET_DIR}. Specify DATASET=x and the entire #{DATASET_DIR}/x directory will be deleted."
    task :clobber => :environment do
      path = dataset_path
      Dir.glob(path + '/*.yml').each do |file|
        puts "Deleting #{file.to_s}"
        File.delete file
      end
      
      if path !~ /dataset$/
        puts "Deleting directory #{path}"
        Dir.rmdir path
      end
      puts "Done"
    end
    
    private
    def dataset_path
      DATASET_DIR + (ENV['DATASET'] ? "/#{ENV['DATASET']}" : "")
    end
  end
end


