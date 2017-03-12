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
    after_date = DateTime.new(2016, 8, 26, 23)
    before_date = after_date + Rational(1, 24)
    db_connect
    @urls = []
    @file_contents = []
    # init_temp_file
    create_urls(after_date, before_date)
    get_data
    # gz = open('http://data.githubarchive.org/2015-01-01-12.json.gz')
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
    temp = File.new('temp.json', 'w')
    copy_to_temp(download, temp)
    # @urls.each do |url|
    #   download = open(url)
    #   temp = File.new('temp.json', 'w')
    # end
  end

  def copy_to_temp(download, temp)
    Zlib::GzipReader.open(download) do | input_stream |
      File.open(temp, "w") do |output_stream|
        IO.copy_stream(input_stream, output_stream)
      end
    end
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
        conn.exect('CREATE TABLE events (id integer, data json )')
        conn
      end
  end
end

GitArchive.new(ARGV[0], ARGV[1], ARGV[2], ARGV[3])

