require 'mkmf'

have_header('ruby.h') or raise 'could not find ruby.h'

def extconf_main
  $objs = %w(cli.o)
  dir_config 'cli'
  if have_library cliv2_libname
    create_makefile 'teradata-cli/cli'
  end
end

def cliv2_libname
  case RUBY_PLATFORM
  when /mswin32|mingw/ then 'wincli32'
  when /mswin64/ then 'wincli64'
  else
    'cliv2'
  end
end

extconf_main
