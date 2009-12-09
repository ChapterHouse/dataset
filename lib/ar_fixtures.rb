require 'active_record/fixtures'

#Patch the fixtures.rb to allow for names to be used in fixtures when associations exist.
#Example:
# consumer_id: 1
#becomes
# consumer: :joe_the_consumer
class Fixtures

  class LoadFailure < Fixture::FormatError
  end

  # Parses the loaded fixture hash to find any obvious association names
  # where the name of the field matches the name of another table.
  # For example, if the table set contains users and this fixture has
  # a column user containing a symbol, we attempt to find the id from 
  # the foreign table and set the column user_id with this value.
  # Note this is highly opinionated and does not take into account
  # actual associations from your application.
  def self.parse_foreign_keys(fixtures, table_names)
    fixtures.each do |table,fixture_data| # ['table_name' => [fixture_data]]
      fixture_data.each do |fixture| # ['fixture_name', <fixture object>]
        
        fixture[1].to_hash.each do |key,value|
          
          # search through the fields in your fixture for matching, singular table names
          if value.is_a?Symbol # and table_names.include?(plur = key.pluralize) this was removed to manage it smart

            #
            # try to find the ActiveRecord Class to obtain if defined foreign_keys
            # 
            
            # first try to find naturally
            unless table_names.include?(plur = key.pluralize)
              # 
              # We have to work a little more
              # 
  
              klass = table.classify.constantize

              reflection = klass.reflections[key.to_sym]
              if reflection then
                class_name = reflection.options[:class_name]
              
                #
                # Now with the real class_name we will try to find it on fixtures
                #
                if table_names.include?(class_name)
                  plur = class_name.tableize
                  unless fixtures[plur]
                    class_name = eval("#{class_name}").base_class.to_s
                    plur = class_name.tableize
                  end
                end
              elsif !klass.reflections[key.to_s[0..-4].to_sym].nil? then
                #Print this out for now as the exception raised seems to dissapear in some IDEs or xterms.
                $stderr.puts "Unable to translate field \"#{key}\" for :#{fixture.first} in #{table}.yml. Perhaps you meant to use \"#{key.to_s[0..-4]}: :#{value}\"?"
                raise Fixtures::LoadFailure, "Unable to translate field \"#{key}\" for :#{fixture.first} in #{table}.yml. Perhaps you meant to use #{key.to_s[0..-4]}?"
              end
            end
            if fixtures[plur] and result = fixtures[plur][value.to_s] and id = result.to_hash['id']
              fixture[1].to_hash[key+"_id"] = id # luckily it's all pass-by-reference here
              fixture[1].to_hash.delete(key)     # don't need the old key any more
            else
              #Print this out for now as the exception raised seems to dissapear in some IDEs or xterms.
              $stderr.puts "Unable to load \"#{key}: :#{value}\" for :#{fixture.first} in #{table}.yml"
              raise Fixtures::LoadFailure, "Unable to load \"#{key}: :#{value}\" for :#{fixture.first} in #{table}.yml"
            end
          end
        end
      end
    end
  end

  def self.create_fixtures(fixtures_directory, table_names, class_names = {})
    table_names = [table_names].flatten.map { |n| n.to_s }
    connection = block_given? ? yield : ActiveRecord::Base.connection

    ActiveRecord::Base.silence do
      fixtures_map = {}
      fixtures = table_names.map do |table_name|
        fixtures_map[table_name] = Fixtures.new(connection, File.split(table_name.to_s).last, class_names[table_name.to_sym], File.join(fixtures_directory, table_name.to_s))
      end               
      all_loaded_fixtures.merge! fixtures_map  

      connection.transaction do
        self.parse_foreign_keys all_loaded_fixtures, table_names
        fixtures.reverse.each { |fixture| fixture.delete_existing_fixtures }
        fixtures.each { |fixture| fixture.insert_fixtures }

        # Cap primary key sequences to max(pk).
        if connection.respond_to?(:reset_pk_sequence!)
          table_names.each do |table_name|
            connection.reset_pk_sequence!(table_name)
          end
        end
      end

      return fixtures.size > 1 ? fixtures : fixtures.first
    end
  end
end

# Extension to make it easy to read and write data to a file.
class ActiveRecord::Base
  class << self

    # Write a fixture file using existing data in the database.
    #
    # Will be written to +db/dataset/table_name.yml+ by default, but +path+ can be 
    # over-ridden. Fixture can be restricted to +limit+ records.
    def to_fixture(path="db/dataset", limit=nil)
      opts = {}
      opts[:limit] = limit if limit
      
      write_file(File.expand_path("#{path}/#{safe_table_name}.yml", RAILS_ROOT), 
          self.find(:all, opts).inject({}) { |hsh, record| 
          if record.respond_to?(:id) && !record.id.nil?
            begin
              hsh.merge("#{safe_table_name.singularize}_#{'%05i' % record.id}" => record.attributes) 
            rescue
              hsh.merge("#{safe_table_name.singularize}_#{record.id}" => record.attributes) 
            end
          else
            hsh.merge("#{safe_table_name.singularize}_#{hsh.keys.size}" => record.attributes) 
          end
            }.to_yaml(:SortKeys => true))
      habtm_to_fixture(path)
    end

    def data_to_fixture(path="db/dataset", &block)
      
      data = yield(path)
      write_file(File.expand_path("#{path}/#{safe_table_name}.yml", RAILS_ROOT), 
          data.inject({}) { |hsh, record| 
          if record.respond_to?(:id) && !record.id.nil?
            begin
              hsh.merge("#{safe_table_name.singularize}_#{'%05i' % record.id}" => record.attributes) 
            rescue
              hsh.merge("#{safe_table_name.singularize}_#{record.id}" => record.attributes) 
            end
          else
            hsh.merge("#{safe_table_name.singularize}_#{hsh.keys.size}" => record.attributes) 
          end
            }.to_yaml(:SortKeys => true))
    end

    private
    
    def safe_table_name
      table_name.gsub('.', '_')
    end
    
    # Write the habtm association table
    def habtm_to_fixture(path)
      joins = self.reflect_on_all_associations.select { |j|
        j.macro == :has_and_belongs_to_many
      }
      joins.each do |join|
        hsh = {}
        connection.select_all("SELECT * FROM #{join.options[:join_table]}").each_with_index { |record, i|
          hsh["join_#{'%05i' % i}"] = record
        }
        write_file(File.expand_path("#{path}/#{join.options[:join_table]}.yml", RAILS_ROOT), hsh.to_yaml(:SortKeys => true))
      end
    end
  
    def write_file(path, content) # :nodoc:
      f = File.new(path, "w+")
      f.puts content
      f.close
    end
  end
end

# Over-ride the yaml sortkeys bug (http://code.whytheluckystiff.net/syck/ticket/3) using the provided patch
# This is actually the default behavior for older (<=1.8.2) versions of ruby
class Hash
  def to_yaml( opts = {} )
    YAML::quick_emit( object_id, opts ) do |out|
      out.map( taguri, to_yaml_style ) do |map|
        sorted_keys = keys
        sorted_keys = begin
          sorted_keys.sort
        rescue
          sorted_keys.sort_by {|k| k.to_s} rescue sorted_keys
        end
        sorted_keys.each do |k|
          map.add( k, fetch(k) )
        end
      end
    end
  end
end
