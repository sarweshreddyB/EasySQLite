
Pod::Spec.new do |s|
  s.name             = 'EasySQLite'
  s.version          = '0.1.0'
  s.summary          = 'Helps you create a DB and perform db operations like insert delete and update and select'
 
  s.description      = <<-DESC
Helps you create a DB and perform db operations like insert delete and update and select
                       DESC
 
  s.homepage         = 'https://github.com/<YOUR GITHUB USERNAME>/EasySQLite'
  s.license          = { :type => 'MIT', :file => 'LICENSE' }
  s.author           = { '<YOUR NAME HERE>' => '<YOUR EMAIL HERE>' }
  s.source           = { :git => 'https://github.com/sarweshreddyB/EasySQLite.git', :tag => s.version.to_s }
 
  s.ios.deployment_target = '10.0'
  s.source_files = 'EasySQLite/EasySQLite.swift'
 
end