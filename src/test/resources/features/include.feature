Feature: Include Aspect with params

  Scenario: Base test for include
   Given I wait '3' seconds
   And I save '1' in variable 'test'

  @include(feature:includeTemplate.feature,scenario:includeAspect with params,params:[paramtest:1])
  Scenario: Test scenario include with param

  @include(feature:includeTemplate.feature,scenario:includeAspect with params,params:[paramtest:!{test}])
  Scenario: Test scenario include env save previously

  @include(feature:includeTemplate.feature,scenario:includeAspect with params,params:[paramtest:${INCLUDE}])
  Scenario: Test scenario include enviroment passed by argument

