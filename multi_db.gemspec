# coding: utf-8
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = %q{multi_db}
  s.version = "0.3.1"
  s.rubyforge_project = "multi_db"
  s.rubygems_version = %q{1.3.1}

  s.homepage = "http://github.com/schoefmax/multi_db"
  s.authors = ["Maximilian Sch\303\266fmann"]
  s.email = "max@pragmatic-it.de"
  s.description = "Connection proxy for ActiveRecord for single master / multiple slave database deployments"
  s.summary = "Connection proxy for ActiveRecord for single master / multiple slave database deployments"

  s.has_rdoc = true
  s.extra_rdoc_files = ["LICENSE", "README.rdoc"]
  s.rdoc_options = ["--line-numbers", "--inline-source", "--title", "multi_db", "--main", "README.rdoc"]

  s.files = `git ls-files`.split("\n")
  s.require_paths = ["lib"]

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'mysql2', '~> 0.3.10'

  s.add_runtime_dependency 'activerecord', '~> 3.2.8'
  s.add_runtime_dependency 'tlattr_accessors', '~> 0.0.3'
end
