#Licensed to the Apache Software Foundation (ASF) under one
#or more contributor license agreements.  See the NOTICE file
#distributed with this work for additional information
#regarding copyright ownership.  The ASF licenses this file
#to you under the Apache License, Version 2.0 (the
#"License"); you may not use this file except in compliance
#with the License.  You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

import sys

sampleText = "Hello World!"

if __name__=="__main__":
  args = sys.argv[1:]
  if args[0] == "1":
    # print sample text to stderr
    sys.stdout.write(sampleText)

  elif args[0] == "2":
    # print sample text to stderr 
    sys.stderr.write(sampleText)

  # Add any other helper programs here, with different values for args[0]
  pass

