require "bundler/gem_tasks"
require "rake/extensiontask"

Rake::ExtensionTask.new do |ext|
  ext.name = 'cli'                        # indicate the name of the extension.
  ext.ext_dir = 'ext/teradata/cli'        # search for 'hello_world' inside it.
  ext.lib_dir = 'lib/teradata'            # put binaries into this folder.
  ext.tmp_dir = 'tmp'                     # temporary folder used during compilation.
  ext.source_pattern = "*.{c,cpp}"        # monitor file changes to allow simple rebuild.
end
