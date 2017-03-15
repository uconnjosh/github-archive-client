require "rubygems"
require 'open-uri'
require 'zlib'
require 'yajl'
require 'pry'
require 'pg'

# gz = open('http://data.githubarchive.org/2015-01-01-12.json.gz')
# js = Zlib::GzipReader.new(gz).read

# binding.pry

# Yajl::Parser.parse(js) do |event|
#   print event
# end



class GitArchive
  def initialize(after_date, before_date, event_type, count)
    after_date = DateTime.new(2016, 9, 17, 23)
    before_date = after_date + Rational(1, 24)
    db_connect

    binding.pry
    # @urls = []
    # @file_contents = []
    # create_urls(after_date, before_date)
    # get_data
    # import_json
  end

  def date_url(date_time_string)
    "http://data.githubarchive.org/#{date_time_string}.json.gz"
  end

  def create_urls(start_time, stop_time)

    while start_time <= stop_time
      year = start_time.year
      month = start_time.month.to_s.length == 1 ? "0#{start_time.month.to_s}" : start_time.month
      day = start_time.day.to_s.length == 1 ? "0#{start_time.day.to_s}" : start_time.day
      hour = start_time.hour.to_s.length == 1 ? "0#{start_time.hour.to_s}" : start_time.hour
      @urls << date_url("#{year}-#{month}-#{day}-#{hour}")
      start_time += Rational(1, 24)
    end
  end

  def get_data
    url = @urls[0]
    download = open(url)
    @temp = File.new('temp.json', 'w')
    puts "copy to temp"
    copy_to_temp(download, @temp)
    puts "convert to json array"
    convert_to_json_array
    puts "importing json!"
    import_json
    puts "done!"
    # @urls.each do |url|
    #   download = open(url)
    #   temp = File.new('temp.json', 'w')
    # end
  end

  def import_json
    @conn.exec(
      <<-IMPORTSQL
        create temporary table temp_json (values text) on commit drop;
        copy temp_json from '/Users/joshpaul/Desktop/github-archive/final_array.json';

        insert into events ("id", "type", "repo")

        select values->>'id' as id,
               values->>'type' as type,
               values->>'repo' as repo


        from (
          select json_array_elements(replace(values::json) as values
          from temp_json
        ) a;
    IMPORTSQL
    )
  end

  def copy_to_temp(download, temp)
    Zlib::GzipReader.open(download) do | input_stream |
      File.open(temp, "w") do |output_stream|
        IO.copy_stream(input_stream, output_stream)
      end
    end
  end

  def convert_to_json_array
    # escaped_quotes_regex = "s/\\"/'\''/g"
    system "tr '\n' ', ' < temp.json > temp_as_array.json"
    system "sed -i '.json' s/.$// temp_as_array.json"
    system "echo '[' | cat - temp_as_array.json > tempfoo && mv tempfoo temp_as_array.json"
    system "echo ']' >> temp_as_array.json"
    system "sed -i '.json' 's/\\r//g' temp_as_array.json"
    system "sed -i '.json' 's/\\n//g' temp_as_array.json"
    # system "sed -i '.json'  's/\\"/'\''/g' temp_as_array.json"

    system "tr -d '\n' < temp_as_array.json > final_array.json"
    # system "tr '\"' '\'\' < temp.json > temp_as_array.json"
    # system "sed -i '.json' 's/\r//g' temp_as_array.json"
    # system "sed -i '.json' 's/\n//g' temp_as_array.json"
    # system "sed -i '.json' 's/\r\n//g' temp_as_array.json"
    # puts "step 5 complete"

    # system "tr '\r' '' < temp_as_array.json > final_array.json"
    # system "tr '\n' '' < final_array.json > final_array2.json"
    puts "json array finalized"

    # tr '\r\n' ' '
    # system "cat temp_as_array.json | tr '\r' ' ' |  tr '\n' ' ' | sed 's/ \{3,\}/ /g' | sed 's/   / /g' > onelinejson.json"
    # system "tr '\r' ' ' < temp_as_array.json > final_array.json"
    # system "tr '\n' ' ' < final_array.json > final_array2.json"
    # system "tr '\n\r' ' ' < final_array2.json > final_array3.json"
    # system "tr '\r\n\r\n' ' ' < final_array3.json > final_array4.json"

    # system "cat temp_as_array.json | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}' | sed -e '$s/,$/]/'  > final_temp.json"
    # puts "done converting to json array"
    # exec "echo -e '[\n$(cat temp6.json)' > foobar.json"
    # exec "echo -e 'task goes here\n$(cat todo.txt)' > todo.txt"

  end

  def db_connect
    @conn ||=
      begin
        conn = PG.connect(dbname: 'gitarchive')
        conn.exec('CREATE TABLE IF NOT EXISTS events (id integer, data json )')
        conn
      rescue
        pg = PG.connect(dbname: 'postgres')
        pg.exec('CREATE DATABASE gitarchive')
        conn = PG.connect(dbname: 'gitarchive')
        conn.exect('CREATE TABLE events (id text, data json )')
        conn
      end
  end
end

GitArchive.new(ARGV[0], ARGV[1], ARGV[2], ARGV[3])

