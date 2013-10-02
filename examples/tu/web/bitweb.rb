# $Id: bitweb.rb 158 2009-05-22 10:13:19Z aamine $

require 'webrick/cgi'
require 'webrick/httpservlet/abstract'
require 'webrick/httpstatus'
begin
  require 'fcgi'
rescue LoadError
end
require 'erb'
require 'yaml'

Socket.do_not_reverse_lookup = true

module BitWeb

  class Error < StandardError; end
  class RequestError < Error; end

  class ValidationError < RequestError
    def initialize(key, message)
      super message
      @key = key
    end

    attr_reader :key
  end


  class Interface

    def initialize(webrick_conf = {})
      @webrick_conf = webrick_conf
      @handler = ($webinterface_context_cache ||= yield)
    end

    # for WEBrick servlet
    def get_instance(server)
      WEBrickServlet.new(server, @handler)
    end

    def main
      if fastcgi?
        FCGI.new(@webrick_conf).main(@handler)
      else
        # CGI, mod_ruby
        CGI.new(@webrick_conf).main(@handler)
      end
    end

    private

    def fastcgi?
      defined?(::FCGI) and ::FCGI.fastcgi?
    end

    def mod_ruby?
      false   # FIXME
    end

  end

  class CGI < ::WEBrick::CGI
    def main(handler)
      @handler = handler
      start
    end

    def do_GET(wreq, wres)
      @handler.handle(wreq).update wres
    end

    alias do_POST do_GET
  end

  class FCGI < CGI
    def main(handler)
      @handler = handler
      ::FCGI.each_cgi_request do |req|
        start req.env, req.in, req.out
      end
    end
  end

  class WEBrickServlet < ::WEBrick::HTTPServlet::AbstractServlet
    def do_GET(wreq, wres)
      @options.first.handle(wreq).update wres
    end

    alias do_POST do_GET
  end


  module HTMLUtils

    private

    ESC = {
      '&' => '&amp;',
      '"' => '&quot;',
      '<' => '&lt;',
      '>' => '&gt;'
    }

    def escape_html(str)
      table = ESC   # optimize
      str.gsub(/[&"<>]/) {|s| table[s] }
    end

    ESCrev = ESC.invert

    def unescape_html(str)
      table = ESCrev   # optimize
      str.gsub(/&\w+;/) {|s| table[s] }
    end

  end


  module ClassUtils

    private

    def find_class_from_current_context(name)
      eval("#{current_class_path}::#{name}")
    rescue NameError => err
      raise RequestError, "unknown screen: #{id.inspect}"
    end

    def current_class_path
      path = self.class.name.split('::')[0..-2]
      path.empty? ? '' : "::#{path.join('::')}"
    end

  end


  class RequestHandler

    include ClassUtils

    def initialize(log, views, models)
      @log = log
      @views = views
      @models = models
      @log.info(self.class) { "application started" }
    end

    def handle(webrick_req)
      respond_to(Request.new(webrick_req))
    rescue WEBrick::HTTPStatus::Status
      raise
    rescue => err
      @log.error(self.class) { "#{err.class}: #{err.message}" }
      return Response.new(ErrorScreen.new(err))
    end

    private

    def respond_to(req)
      @log.info(self.class) {
        "new request [#{req.controller.inspect}/#{req.command.inspect}]"
      }
      ctl = controller_class(req.controller).new(@log, @views, @models)
      res = Response.new(ctl.handle(req))
      @log.debug(self.class) { "controller returned" }
      res
    end

    def controller_class(id)
      find_class_from_current_context("#{id.capitalize}Controller")
    end

  end


  class Controller

    def self.depends(*names)
      ivar_list = names.map {|n| "@#{n}" }.join(', ')
      sym_list = names.map {|n| ":#{n}" }.join(', ')
      module_eval(<<-End, __FILE__, __LINE__ + 1)
        def initialize(log, views, models)
          super log, views
          #{ivar_list}, * = models.values_at(#{sym_list})
        end
      End
    end
    private_class_method :depends

    def initialize(log, views)
      @log = log
      @views = views
    end

    def handle(req)
      mid = "handle_#{req.command}"
      unless respond_to?(mid, true)
        @log.error(self.class) { "unknown command: #{req.command.inspect}" }
        raise RequestError, "unknown command: #{req.command.inspect}"
      end
      @log.debug(self.class) { "dispatch: #{self.class}\##{mid}" }
      __send__(mid, req)
    end

  end


  class Request

    def initialize(wreq)
      @wreq = wreq
    end

    def peer_ipaddr
      @peer_ipaddr ||= @wreq.peeraddr[2]
    end

    def peer_hostname
      @peer_hostname ||= getnameinfo(peer_ipaddr).host
    end

    def getnameinfo(addr)
      NameInfo.new(*Socket.getnameinfo([Socket::AF_UNSPEC, nil, addr]))
    end
    private :getnameinfo

    NameInfo = Struct.new(:host, :port)

    def controller
      path_components[0] or
          raise RequestError, "controller name did not given"
    end

    def command
      path_components[1] or
          raise RequestError, "command name did not given"
    end

    def path_components
      @wreq.path_info.sub(%r<\A/>, '').split('/')
    end

    def [](name)
      get(name)
    end

    def get(name)
      val = @wreq.query[name]
      s = (val && val.to_s)
      if block_given?
        yield(Parameter.new(name, s))
      else
        s
      end
    end

    def parameters(*keys)
      h = {}
      keys.each do |k|
        h[k] = get(k)
      end
      h
    end

    class Parameter
      def initialize(name, val)
        @name = name
        @value = val
      end

      def raw_value
        @value
      end

      def string
        @value.to_s
      end

      def time
        Time.parse(@value.to_s)
      rescue ArgumentError => err
        validation_error "bad time format"
      end

      def date
        Time.parse(@value.to_s)
      rescue ArgumentError => err
        validation_error "bad date format"
      end

      def must_date
        unless %r<\A\d{4}[\-/]\d{1,2}[\-/]\d{1,2}\z> =~ @value
          validation_error "bad date format"
        end
      end

      def must_exist
        unless @value
          validation_error "not exist"
        end
      end

      def must_string
        must_exist
      end

      def must_not_empty
        must_string
        if @value.strip.empty?
          validation_error "is empty"
        end
      end

      def must_match(re)
        unless re =~ @value
          validation_error "bad format"
        end
      end

      def must(msg = 'bad value')
        unless yield(@value)
          validation_error msg
        end
      end

      def validation_error(msg)
        raise ValidationError.new(@name, msg)
      end
    end

  end


  class Response

    def initialize(screen)
      @screen = screen
    end

    def update(wres)
      wres.status = @screen.status if @screen.status
      wres['Content-Type'] = @screen.content_type
      body = @screen.body
      wres['Content-Length'] = body.length
      wres.body = body
    end

  end


  class ViewManager

    def initialize(log, template_dir, message_file, base_url, app_base_url = base_url)
      @log = log
      @template_dir = template_dir
      @messages = Messages.load(message_file)
      @base_url = base_url
      @app_base_url = app_base_url
    end

    def new(screen_class, *args)
      screen_class.new(@log, self, *args)
    end

    def run(id, binding)
      erb = ERB.new(load(id))
      erb.filename = id + '.erb'
      erb.result(binding)
    end

    def load(id)
      preproc(File.read("#{@template_dir}/#{id}"))
    end

    def preproc(template)
      template.gsub(/^\.include ([\w\-]+)/) { load($1.untaint) }.untaint
    end
    private :preproc

    def translate_message(key)
      @messages[key]
    end

    def application_url(rel)
      "#{@app_base_url}#{rel}"
    end

    def css_url(rel)
      "#{@base_url}/css/#{rel}"
    end

    def js_url(rel)
      "#{@base_url}/js/#{rel}"
    end

    def image_url(rel)
      "#{@base_url}/images/#{rel}"
    end

  end


  class Messages
    def Messages.load(path)
      new(YAML.load_file(path))
    end

    def initialize(h)
      @messages = h
    end

    def [](key)
      @messages[key] || key
    end
  end


  class Screen

    def status
      nil
    end

  end


  class ErrorScreen < Screen

    include HTMLUtils

    def initialize(error)
      @error = error
    end

    def status
      500
    end

    def content_type
      # IE does not support XHTML, do not return "application/xhtml+xml".
      'text/html'
    end

    def body
      <<-EndHTML
<html>
<head><title>Error</title></head>
<body>
<h1>Error</h1>
<pre>#{escape_html(@error.message)} (#{escape_html(@error.class.name)})
#{@error.backtrace.map {|s| escape_html(s) }.join("\n")}</pre>
</body>
</html>
      EndHTML
    end

  end


  class TemplateScreen < Screen

    def TemplateScreen.new_class(*attrs)
      c = Class.new(self)
      c.attributes(*attrs)
      c
    end

    def TemplateScreen.attributes(*attrs)
      ivar_list = attrs.map {|a| "@#{a}" }.join(', ')
      param_list = attrs.join(', ')
      module_eval(<<-End, __FILE__, __LINE__ + 1)
        def initialize(log, views, #{attrs.join(', ')})
          super log, views
          #{ivar_list} = #{param_list}
        end
      End
      module_eval {
        attrs.each do |a|
          attr_reader a
        end
      }
    end

    include HTMLUtils

    def initialize(log, views)
      @log = log
      @views = views
    end

    def content_type
      # IE does not support XHTML, do not return "application/xhtml+xml".
      "text/html"
    end

    def body
      @log.debug(self.class) { "running template: #{template_id}" }
      @views.run(template_id, binding)
    end

    private

    def template_id
      c, s = self.class.name.split('::')[-2,2]
      c.downcase.sub(/controller\z/, '') + '/' + s.downcase.sub(/screen\z/, '')
    end

    def _(key)
      @views.translate_message(key)
    end

    def app_url(rel)
      @views.application_url(rel)
    end

    def url(rel)
      string(app_url(rel))
    end

    def css_url(rel)
      @views.css_url(rel)
    end

    def css(rel)
      string(css_url(rel))
    end

    def js_url(rel)
      @views.js_url(rel)
    end

    def js(rel)
      string(js_url(rel))
    end

    def image_url(rel)
      @views.image_url(rel)
    end

    def img(rel)
      string(image_url(rel))
    end

    def int(value)
      value.to_i.to_s
    end

    def string(value)
      escape_html(value.to_s)
    end

    alias h string

    def multiline(value)
      value.to_s.lines.map {|s| escape_html(s.strip) }.join('<br />')
    end

    def date(t)
      t.strftime('%Y-%m-%d')
    end

    def timestamp(t)
      t.strftime('%Y-%m-%d %H:%M:%S')
    end
  end


  class Models < Struct
    def values_at(*keys)
      keys.map {|k| self[k] }
    end
  end

end
