Feature: Include Template

  Scenario:includeAspect with params
    Then I run 'echo <paramtest>' locally
    And the command output contains '1'

