/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfsproxy;

import java.util.ArrayList;
import java.util.Hashtable;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;

class DummyLdapContext extends InitialLdapContext {
  class ResultEnum<T> implements NamingEnumeration<T> {
    private ArrayList<T> rl;

    public ResultEnum() {
      rl = new ArrayList<T>();
    }

    public ResultEnum(ArrayList<T> al) {
      rl = al;
    }

    public boolean hasMoreElements() {
      return !rl.isEmpty();
    }

    public T nextElement() {
      T t = rl.get(0);
      rl.remove(0);
      return t;
    }

    public boolean hasMore() throws NamingException {
      return !rl.isEmpty();
    }

    public T next() throws NamingException {
      T t = rl.get(0);
      rl.remove(0);
      return t;
    }

    public void close() throws NamingException {
    }
  }

  public DummyLdapContext() throws NamingException {
  }

  public DummyLdapContext(Hashtable<?, ?> environment, Control[] connCtls)
      throws NamingException {
  }

  public NamingEnumeration<SearchResult> search(String name,
      Attributes matchingAttributes, String[] attributesToReturn)
      throws NamingException {
    System.out.println("Searching Dummy LDAP Server Results:");
    if (!"ou=proxyroles,dc=mycompany,dc=com".equalsIgnoreCase(name)) {
      System.out.println("baseName mismatch");
      return new ResultEnum<SearchResult>();
    }
    if (!"cn=127.0.0.1".equals((String) matchingAttributes.get("uniqueMember")
        .get())) {
      System.out.println("Ip address mismatch");
      return new ResultEnum<SearchResult>();
    }
    BasicAttributes attrs = new BasicAttributes();
    BasicAttribute uidAttr = new BasicAttribute("uid", "testuser");
    attrs.put(uidAttr);
    BasicAttribute groupAttr = new BasicAttribute("userClass", "testgroup");
    attrs.put(groupAttr);
    BasicAttribute locAttr = new BasicAttribute("documentLocation", "/testdir");
    attrs.put(locAttr);
    SearchResult sr = new SearchResult(null, null, attrs);
    ArrayList<SearchResult> al = new ArrayList<SearchResult>();
    al.add(sr);
    NamingEnumeration<SearchResult> ne = new ResultEnum<SearchResult>(al);
    return ne;
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    DummyLdapContext dlc = new DummyLdapContext();
    String baseName = "ou=proxyroles,dc=mycompany,dc=com";
    Attributes matchAttrs = new BasicAttributes(true);
    String[] attrIDs = { "uid", "documentLocation" };
    NamingEnumeration<SearchResult> results = dlc.search(baseName, matchAttrs,
        attrIDs);
    if (results.hasMore()) {
      SearchResult sr = results.next();
      Attributes attrs = sr.getAttributes();
      for (NamingEnumeration ne = attrs.getAll(); ne.hasMore();) {
        Attribute attr = (Attribute) ne.next();
        if ("uid".equalsIgnoreCase(attr.getID())) {
          System.out.println("User ID = " + attr.get());
        } else if ("documentLocation".equalsIgnoreCase(attr.getID())) {
          System.out.println("Document Location = ");
          for (NamingEnumeration e = attr.getAll(); e.hasMore();) {
            System.out.println(e.next());
          }
        }
      }
    }
  }
}
