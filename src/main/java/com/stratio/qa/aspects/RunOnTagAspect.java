package com.stratio.qa.aspects;

import com.stratio.qa.utils.ThreadProperty;
import gherkin.formatter.model.Comment;
import gherkin.formatter.model.Tag;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Aspect
public class RunOnTagAspect {

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getCanonicalName());

    @Pointcut("execution (gherkin.formatter.model.Scenario.new(..)) && " +
              "args(comments, tags, keyword, name, description, line, id)")
    protected void AddRunOnTagPointcutScenario(List<Comment> comments, List<Tag> tags, String keyword, String name,
                                               String description, Integer line, String id) {
    }

    /**
     * Allows conditional scenario execution.
     * <p>
     * If the scenario contains the following tag:
     * @runOnEnv(param): The scenario will only be executed if the param is defined when test is launched.
     * <p>
     * <p>
     * More than one param can be passed in the tag. To do so, the params must be comma separated:
     * @runOnEnv(param1,param2,param3): The scenario will only be executed if ALL the params are defined.
     * <p>
     * Additionally, if the scenario contains the following tag:
     * @skipOnEnv(param): The scenario will be omitted if the param is defined when test is launched.
     * <p>
     * <p>
     * More than one param can be passed in the tag. To do so, the params must be comma separated:
     * @skipOnEnv(param1,param2,param3): The scenario will omitted if ANY of params are defined. (OR)
     * <p>
     * Or in separated lines to force ALL of the params to be defined in order to omit the execution:
     * <p>
     * @skipOnEnv(param1)
     * @skipOnEnv(param2)
     * @skipOnEnv(param3): The scenario will omitted if ALL of params are defined. (AND)
     * <p>
     *
     * @param pjp
     * @param comments
     * @param tags
     * @param keyword
     * @param name
     * @param description
     * @param line
     * @param id
     * @throws Throwable
     */
    @Around(value = "AddRunOnTagPointcutScenario(comments, tags, keyword, name, description, line, id)")
    public void aroundAddRunOnTagPointcut(ProceedingJoinPoint pjp, List<Comment> comments, List<Tag> tags,
                                                  String keyword, String name, String description, Integer line, String id) throws Throwable {


        for (Tag tag : tags) {
            if (tag.getName().contains("@runOnEnv")) {
                if (!checkParams(getParams(tag.getName()))) {
                    tags.add(new Tag("@ignore(runOnEnvs)", line));
                    ThreadProperty.set("skippedOnParams" + pjp.getArgs()[3].toString(), "true");
                    break;
                }
            } else if (tag.getName().contains("@skipOnEnv")){
                if (checkParams(getParams(tag.getName()))) {
                    tags.add(new Tag("@ignore(runOnEnvs)", line));
                    ThreadProperty.set("skippedOnParams" + pjp.getArgs()[3].toString(), "true");
                    break;
                }
            }
        }
    }

    /*
    * Returns a string array of params
    */
    public String[] getParams(String s) throws Exception {
        String[] vals;
        if (s.isEmpty()){
            throw new Exception("-> Error while parsing params. Params must be at least one");
        } else {
            vals = s.substring((s.lastIndexOf("(") + 1), (s.length()) - 1).split(",");
        }
        return vals;
    }

   /*
    * Checks if every param in the array of strings is defined
    */
    public boolean checkParams(String[] params) {
        for(int i = 0; i < params.length; i++) {
            if (System.getProperty(params[i], "").isEmpty()){
                return false;
            }
        }
        return true;
    }
}