require File.expand_path(File.dirname(__FILE__) + '/spec_helper')

class SpecTest < ActiveRecord::Base; end

describe 'ActiveRecord extensions' do
  describe 'establish_connection' do
    before do
      SpecTest.proxy_spec = nil
    end

    it 'should save the spec if it is a string' do
      spec = 'test'
      SpecTest.establish_connection spec
      SpecTest.proxy_spec.should == spec
    end

    it 'should save the spec as a string if it is a symbol' do
      spec = :test
      SpecTest.establish_connection spec
      SpecTest.proxy_spec.should == spec.to_s
    end

    it 'should not save non-string and non-symbol specs' do
      SpecTest.establish_connection {}
      SpecTest.proxy_spec.should be_nil
    end

    it 'should not retain saved spec when setting to a non-string or non-symbol spec' do
      SpecTest.establish_connection :test
      SpecTest.establish_connection {}
      SpecTest.proxy_spec.should be_nil
    end

    it 'should not save invalid specs' do
      SpecTest.establish_connection :fake rescue nil
      SpecTest.proxy_spec.should be_nil
    end

  end
end
