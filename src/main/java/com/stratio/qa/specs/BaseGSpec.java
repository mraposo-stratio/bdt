/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.qa.specs;

import com.stratio.qa.assertions.Assertions;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class BaseGSpec {

    protected CommonG commonspec;

    /**
     * Constructor BaseGSpec.
     *
     * @return commonspec
     */
    public CommonG getCommonSpec() {
        return this.commonspec;
    }

    public void writeInFile(String json, String fileName) throws Exception {

        // Create file (temporary) and set path to be accessible within test
        File tempDirectory = new File(System.getProperty("user.dir") + "/target/test-classes/");
        String absolutePathFile = tempDirectory.getAbsolutePath() + "/" + fileName;
        commonspec.getLogger().debug("Creating file {} in 'target/test-classes'", absolutePathFile);
        // Note that this Writer will delete the file if it exists
        Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(absolutePathFile), StandardCharsets.UTF_8));
        try {
            out.write(json);
        } catch (Exception e) {
            commonspec.getLogger().error("Custom file {} hasn't been created:\n{}", absolutePathFile, e.toString());
        } finally {
            out.close();
        }

        Assertions.assertThat(new File(absolutePathFile).isFile());
    }
}
