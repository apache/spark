/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.log4j;

import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SensitivePatternLayout extends PatternLayout {

  private static final String PREFIX_GROUP_NAME = "prefix";
  private static final String SENSITIVE_GROUP_NAME = "sensitive";
  private static final String MASK = "******";
  private static final Pattern SENSITIVE_PATTERN = Pattern.compile(
      String
          .format("(?<%s>password\\s*[:=])(?<%s>[^,.!]*)",
              PREFIX_GROUP_NAME, SENSITIVE_GROUP_NAME),
      Pattern.CASE_INSENSITIVE);

  @Override
  public String format(LoggingEvent event) {
    if (event.getMessage() instanceof String) {
      String maskedMessage = mask(event.getRenderedMessage());

      Throwable throwable = event.getThrowableInformation() != null
          ? event.getThrowableInformation().getThrowable()
          : null;
      LoggingEvent maskedEvent = new LoggingEvent(event.fqnOfCategoryClass,
          Logger.getLogger(event.getLoggerName()), event.timeStamp, event.getLevel(), maskedMessage,
          throwable);

      return super.format(maskedEvent);
    }
    return super.format(event);
  }

  private String mask(String message) {
    Matcher matcher = SENSITIVE_PATTERN.matcher(message);
    if (matcher.find()) {
      return matcher.replaceAll(String.format("${%s}%s", PREFIX_GROUP_NAME, MASK));
    }
    return message;
  }
}
