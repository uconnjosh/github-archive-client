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
          select json_array_elements(values::json) as values
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
    system "cat temp.json| jq --slurp '[.[] | {id: .id, type: .type, repo: .repo.name }]' > temp_as_array.json"
    system "tr -d '\n' < temp_as_array.json > final_array.json"
  end

  def db_connect
    @conn ||=
      begin
        conn = PG.connect(dbname: 'gitarchivetwo')
        conn.exec('CREATE TABLE IF NOT EXISTS events (id text, type text, repo text)')
        conn
      rescue
        pg = PG.connect(dbname: 'postgres')
        pg.exec('CREATE DATABASE gitarchivetwo')
        conn = PG.connect(dbname: 'gitarchivetwo')
        conn.exect('CREATE TABLE events (id text, type text, repo text)')
        conn
      end
  end

  def query_for_type(type="PushEvent")
    # then sort by ids to get count!
    @conn.exec("SELECT ALL from events WHERE type=#{type}")
  end

  def distinct_repos
    @repos ||=
      @conn.exec("SELECT DISTINCT repo FROM events;").to_a
  end

  def event_count_by_repo(repo, type="PushEvent")
    @conn.exec("SELECT COUNT(DISTINCT id) FROM events where type='#{type}' AND repo='#{repo}';")
  end

end

GitArchive.new(ARGV[0], ARGV[1], ARGV[2], ARGV[3])

