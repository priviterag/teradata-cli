#!/usr/bin/env ruby
#
# $Id: standalone.rb 994 2004-12-08 15:16:41Z aamine $
#
# Stand-alone server based on WEBrick
#

$KCODE = 'UTF-8' unless defined?(Encoding)
$LOAD_PATH.push '.' unless $LOAD_PATH.include?('.')

require 'tdwalker'
require 'teradata'
require 'bitweb'
require 'bitdao'
require 'bitdao/teradata'
require 'webrick'
require 'optparse'
require 'logger'

def main
  @port = 10080
  @host = 'localhost'
  @appdir = File.expand_path(File.dirname($0))
  @logon_string = nil
  @debug = false

  parser = OptionParser.new
  parser.banner = "#{$0} [--port=NUM] [--debug]"
  parser.on('-p', '--port=NUM', 'Listening port number') {|num|
    @port = num.to_i
  }
  parser.on('-h', '--hostname=NAME', 'Server host name') {|name|
    @host = name
  }
  parser.on('-l', '--logon-string=STR', 'Teradata logon string') {|str|
    @logon_string = str
  }
  parser.on('--[no-]debug', 'Debug mode') {|flag|
    @debug = flag
  }
  parser.on('--help', 'Prints this message and quit') {
    puts parser.help
    exit 0
  }
  begin
    parser.parse!
    unless @logon_string
      $stderr.puts "--logon-string option is mandatory"
      exit 1
    end
  rescue OptionParser::ParseError => err
    $stderr.puts err.message
    $stderr.puts parser.help
    exit 1
  end
  start_server
end

def start_server
  server = WEBrick::HTTPServer.new(
    :Port => @port,
    :AccessLog => [
      [ $stderr, WEBrick::AccessLog::COMMON_LOG_FORMAT  ],
      [ $stderr, WEBrick::AccessLog::REFERER_LOG_FORMAT ],
      [ $stderr, WEBrick::AccessLog::AGENT_LOG_FORMAT   ],
    ],
    :Logger => WEBrick::Log.new($stderr, WEBrick::Log::DEBUG)
  )
  server.mount '/', BitWeb::WEBrickServlet, request_handler()
  server.mount '/htdocs/', WEBrick::HTTPServlet::FileHandler, "#{@appdir}/htdocs"
  trap(:INT) { server.shutdown }
  server.start
end

def request_handler
  log = Logger.new($stderr)
  TDWalker::RequestHandler.new(
    log,
    TDWalker::ViewManager.new(
      log,
      "#{@appdir}/template",
      "#{@appdir}/messages",
      "http://#{@host}:#{@port}"
    ),
    TDWalker::Models.new(
      TDWalker::DAO.new(
        log,
        Teradata.connect(@logon_string)
      )
    )
  )
end

main
