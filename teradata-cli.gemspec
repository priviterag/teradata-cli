# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'teradata/cli/version'

Gem::Specification.new do |spec|
  spec.name          = "teradata-cli"
  spec.version       = Teradata::Cli::VERSION
  spec.authors       = ["Giuseppe Privitera"]
  spec.email         = ["priviterag@gmail.com"]
  spec.description   = %q{ruby extension for Teradata Cliv2}
  spec.summary       = %q{ruby extension for Teradata Cliv2}
  spec.homepage      = ""
  spec.license       = "LGPL2"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.extensions    = ["ext/teradata/cli/extconf.rb"]
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
end
