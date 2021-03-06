Feature: Convert provided date into timestamp

  Scenario: [Convert dates] Convert date to timestamp UTC
    When I convert date '2020-08-08' in format 'yyyy-MM-dd' into timestamp and save the value in environment variable 'timestamp'
    And '!{timestamp}' is '1596844800000'
    When I convert date '2020-08-08' in format 'yyyy-MM-dd' with timezone 'GMT+1' into timestamp and save the value in environment variable 'timestamp'
    And '!{timestamp}' is '1596841200000'
    When I convert date '2020-08-08 10:10:10' in format 'yyyy-MM-dd HH:mm:ss' with timezone 'GMT+1' into timestamp and save the value in environment variable 'timestamp'
    And '!{timestamp}' is '1596877810000'
    When I convert date '2020-08-08 09:10:10' in format 'yyyy-MM-dd HH:mm:ss' with timezone 'GMT' into timestamp and save the value in environment variable 'timestamp'
    And '!{timestamp}' is '1596877810000'
    When I convert date '2020-08-08 09:10:10' in format 'yyyy-MM-dd HH:mm:ss' into timestamp and save the value in environment variable 'timestamp'
    And '!{timestamp}' is '1596877810000'
    #When I convert date '2020/08/08 09:10:10' in format 'yyyy-MM-dd HH:mm:ss' into timestamp and save the value in environment variable 'timestamp'
    #When I convert date '2020-08-08 09:10:10' in format 'fake' into timestamp and save the value in environment variable 'timestamp'