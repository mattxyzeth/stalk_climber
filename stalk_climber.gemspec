# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'stalk_climber/version'

Gem::Specification.new do |spec|
  spec.name          = 'stalk_climber'
  spec.version       = StalkClimber::VERSION
  spec.authors       = ['Gemerald Beanstalk']
  spec.email         = ['gemeraldbeanstalk@gmail.com']
  spec.description   = %q{Improved sequential access to Beanstalk}
  spec.summary       = %q{StalkClimber is a Ruby library allowing improved sequential access to Beanstalk via a job cache.}
  spec.homepage      = 'https://github.com/gemeraldbeanstalk/stalk_climber'
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^test/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.3'
  spec.add_development_dependency 'rake'

  spec.add_dependency 'beaneater'
end
