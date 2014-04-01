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
package org.apache.hadoop.fs.permission;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import junit.framework.TestCase;

import static org.apache.hadoop.fs.permission.FsAction.*;

public class TestFsPermission extends TestCase {
  public void testFsAction() {
    //implies
    for(FsAction a : FsAction.values()) {
      assertTrue(ALL.implies(a));
    }
    for(FsAction a : FsAction.values()) {
      assertTrue(a == NONE? NONE.implies(a): !NONE.implies(a));
    }
    for(FsAction a : FsAction.values()) {
      assertTrue(a == READ_EXECUTE || a == READ || a == EXECUTE || a == NONE?
          READ_EXECUTE.implies(a): !READ_EXECUTE.implies(a));
    }

    //masks
    assertEquals(EXECUTE, EXECUTE.and(READ_EXECUTE));
    assertEquals(READ, READ.and(READ_EXECUTE));
    assertEquals(NONE, WRITE.and(READ_EXECUTE));

    assertEquals(READ, READ_EXECUTE.and(READ_WRITE));
    assertEquals(NONE, READ_EXECUTE.and(WRITE));
    assertEquals(WRITE_EXECUTE, ALL.and(WRITE_EXECUTE));
  }

  /**
   * Ensure that when manually specifying permission modes we get
   * the expected values back out for all combinations
   */
  public void testConvertingPermissions() {
    for(short s = 0; s < 01777; s++) {
      assertEquals(s, new FsPermission(s).toShort());
    }

    short s = 0;

    for(boolean sb : new boolean [] { false, true }) {
      for(FsAction u : FsAction.values()) {
        for(FsAction g : FsAction.values()) {
          for(FsAction o : FsAction.values()) {
            FsPermission f = new FsPermission(u, g, o, sb);
            assertEquals(s, f.toShort());
            s++;
          }
        }
      }
    }
  }

  public void testStickyBitToString() {
    // Check that every permission has its sticky bit represented correctly
    for(boolean sb : new boolean [] { false, true }) {
      for(FsAction u : FsAction.values()) {
        for(FsAction g : FsAction.values()) {
          for(FsAction o : FsAction.values()) {
            FsPermission f = new FsPermission(u, g, o, sb);
            if(f.getStickyBit() && f.getOtherAction().implies(EXECUTE))
              assertEquals('t', f.toString().charAt(8));
            else if(f.getStickyBit() && !f.getOtherAction().implies(EXECUTE))
              assertEquals('T', f.toString().charAt(8));
            else if(!f.getStickyBit()  && f.getOtherAction().implies(EXECUTE))
              assertEquals('x', f.toString().charAt(8));
            else
              assertEquals('-', f.toString().charAt(8));
          }
        }
      }
    }
  }

  public void testFsPermission() {
    String symbolic = "-rwxrwxrwx";
    StringBuilder b = new StringBuilder("-123456789");
    for(int i = 0; i < (1<<9); i++) {
      for(int j = 1; j < 10; j++) {
        b.setCharAt(j, '-');
      }
      String binary = Integer.toBinaryString(i);
      int len = binary.length();
      for(int j = 0; j < len; j++) {
        if (binary.charAt(j) == '1') {
          int k = 9 - (len - 1 - j);
          b.setCharAt(k, symbolic.charAt(k));
        }
      }
      assertEquals(i, FsPermission.valueOf(b.toString()).toShort());
    }
  }

  public void testUMaskParser() throws IOException {
    Configuration conf = new Configuration();
    
    // Ensure that we get the right octal values back for all legal values
    for(FsAction u : FsAction.values()) {
      for(FsAction g : FsAction.values()) {
        for(FsAction o : FsAction.values()) {
          FsPermission f = new FsPermission(u, g, o);
          String asOctal = String.format("%1$03o", f.toShort());
          conf.set(FsPermission.UMASK_LABEL, asOctal);
          FsPermission fromConf = FsPermission.getUMask(conf);
          assertEquals(f, fromConf);
        }
      }
    }
  }

  public void testSymbolicUmasks() {
    Configuration conf = new Configuration();
    
    // Test some symbolic to octal settings
    // Symbolic umask list is generated in linux shell using by the command:
    // umask 0; umask <octal number>; umask -S
    String [][] symbolic = new String [][] { 
        {"a+rw", "111",},
        {"u=rwx,g=rwx,o=rwx", "0",},
        {"u=rwx,g=rwx,o=rw", "1",},
        {"u=rwx,g=rwx,o=rx", "2",},
        {"u=rwx,g=rwx,o=r", "3",},
        {"u=rwx,g=rwx,o=wx", "4",},
        {"u=rwx,g=rwx,o=w", "5",},
        {"u=rwx,g=rwx,o=x", "6",},
        {"u=rwx,g=rwx,o=", "7",},
        {"u=rwx,g=rw,o=rwx", "10",},
        {"u=rwx,g=rw,o=rw", "11",},
        {"u=rwx,g=rw,o=rx", "12",},
        {"u=rwx,g=rw,o=r", "13",},
        {"u=rwx,g=rw,o=wx", "14",},
        {"u=rwx,g=rw,o=w", "15",},
        {"u=rwx,g=rw,o=x", "16",},
        {"u=rwx,g=rw,o=", "17",},
        {"u=rwx,g=rx,o=rwx", "20",},
        {"u=rwx,g=rx,o=rw", "21",},
        {"u=rwx,g=rx,o=rx", "22",},
        {"u=rwx,g=rx,o=r", "23",},
        {"u=rwx,g=rx,o=wx", "24",},
        {"u=rwx,g=rx,o=w", "25",},
        {"u=rwx,g=rx,o=x", "26",},
        {"u=rwx,g=rx,o=", "27",},
        {"u=rwx,g=r,o=rwx", "30",},
        {"u=rwx,g=r,o=rw", "31",},
        {"u=rwx,g=r,o=rx", "32",},
        {"u=rwx,g=r,o=r", "33",},
        {"u=rwx,g=r,o=wx", "34",},
        {"u=rwx,g=r,o=w", "35",},
        {"u=rwx,g=r,o=x", "36",},
        {"u=rwx,g=r,o=", "37",},
        {"u=rwx,g=wx,o=rwx", "40",},
        {"u=rwx,g=wx,o=rw", "41",},
        {"u=rwx,g=wx,o=rx", "42",},
        {"u=rwx,g=wx,o=r", "43",},
        {"u=rwx,g=wx,o=wx", "44",},
        {"u=rwx,g=wx,o=w", "45",},
        {"u=rwx,g=wx,o=x", "46",},
        {"u=rwx,g=wx,o=", "47",},
        {"u=rwx,g=w,o=rwx", "50",},
        {"u=rwx,g=w,o=rw", "51",},
        {"u=rwx,g=w,o=rx", "52",},
        {"u=rwx,g=w,o=r", "53",},
        {"u=rwx,g=w,o=wx", "54",},
        {"u=rwx,g=w,o=w", "55",},
        {"u=rwx,g=w,o=x", "56",},
        {"u=rwx,g=w,o=", "57",},
        {"u=rwx,g=x,o=rwx", "60",},
        {"u=rwx,g=x,o=rw", "61",},
        {"u=rwx,g=x,o=rx", "62",},
        {"u=rwx,g=x,o=r", "63",},
        {"u=rwx,g=x,o=wx", "64",},
        {"u=rwx,g=x,o=w", "65",},
        {"u=rwx,g=x,o=x", "66",},
        {"u=rwx,g=x,o=", "67",},
        {"u=rwx,g=,o=rwx", "70",},
        {"u=rwx,g=,o=rw", "71",},
        {"u=rwx,g=,o=rx", "72",},
        {"u=rwx,g=,o=r", "73",},
        {"u=rwx,g=,o=wx", "74",},
        {"u=rwx,g=,o=w", "75",},
        {"u=rwx,g=,o=x", "76",},
        {"u=rwx,g=,o=", "77",},
        {"u=rw,g=rwx,o=rwx", "100",},
        {"u=rw,g=rwx,o=rw", "101",},
        {"u=rw,g=rwx,o=rx", "102",},
        {"u=rw,g=rwx,o=r", "103",},
        {"u=rw,g=rwx,o=wx", "104",},
        {"u=rw,g=rwx,o=w", "105",},
        {"u=rw,g=rwx,o=x", "106",},
        {"u=rw,g=rwx,o=", "107",},
        {"u=rw,g=rw,o=rwx", "110",},
        {"u=rw,g=rw,o=rw", "111",},
        {"u=rw,g=rw,o=rx", "112",},
        {"u=rw,g=rw,o=r", "113",},
        {"u=rw,g=rw,o=wx", "114",},
        {"u=rw,g=rw,o=w", "115",},
        {"u=rw,g=rw,o=x", "116",},
        {"u=rw,g=rw,o=", "117",},
        {"u=rw,g=rx,o=rwx", "120",},
        {"u=rw,g=rx,o=rw", "121",},
        {"u=rw,g=rx,o=rx", "122",},
        {"u=rw,g=rx,o=r", "123",},
        {"u=rw,g=rx,o=wx", "124",},
        {"u=rw,g=rx,o=w", "125",},
        {"u=rw,g=rx,o=x", "126",},
        {"u=rw,g=rx,o=", "127",},
        {"u=rw,g=r,o=rwx", "130",},
        {"u=rw,g=r,o=rw", "131",},
        {"u=rw,g=r,o=rx", "132",},
        {"u=rw,g=r,o=r", "133",},
        {"u=rw,g=r,o=wx", "134",},
        {"u=rw,g=r,o=w", "135",},
        {"u=rw,g=r,o=x", "136",},
        {"u=rw,g=r,o=", "137",},
        {"u=rw,g=wx,o=rwx", "140",},
        {"u=rw,g=wx,o=rw", "141",},
        {"u=rw,g=wx,o=rx", "142",},
        {"u=rw,g=wx,o=r", "143",},
        {"u=rw,g=wx,o=wx", "144",},
        {"u=rw,g=wx,o=w", "145",},
        {"u=rw,g=wx,o=x", "146",},
        {"u=rw,g=wx,o=", "147",},
        {"u=rw,g=w,o=rwx", "150",},
        {"u=rw,g=w,o=rw", "151",},
        {"u=rw,g=w,o=rx", "152",},
        {"u=rw,g=w,o=r", "153",},
        {"u=rw,g=w,o=wx", "154",},
        {"u=rw,g=w,o=w", "155",},
        {"u=rw,g=w,o=x", "156",},
        {"u=rw,g=w,o=", "157",},
        {"u=rw,g=x,o=rwx", "160",},
        {"u=rw,g=x,o=rw", "161",},
        {"u=rw,g=x,o=rx", "162",},
        {"u=rw,g=x,o=r", "163",},
        {"u=rw,g=x,o=wx", "164",},
        {"u=rw,g=x,o=w", "165",},
        {"u=rw,g=x,o=x", "166",},
        {"u=rw,g=x,o=", "167",},
        {"u=rw,g=,o=rwx", "170",},
        {"u=rw,g=,o=rw", "171",},
        {"u=rw,g=,o=rx", "172",},
        {"u=rw,g=,o=r", "173",},
        {"u=rw,g=,o=wx", "174",},
        {"u=rw,g=,o=w", "175",},
        {"u=rw,g=,o=x", "176",},
        {"u=rw,g=,o=", "177",},
        {"u=rx,g=rwx,o=rwx", "200",},
        {"u=rx,g=rwx,o=rw", "201",},
        {"u=rx,g=rwx,o=rx", "202",},
        {"u=rx,g=rwx,o=r", "203",},
        {"u=rx,g=rwx,o=wx", "204",},
        {"u=rx,g=rwx,o=w", "205",},
        {"u=rx,g=rwx,o=x", "206",},
        {"u=rx,g=rwx,o=", "207",},
        {"u=rx,g=rw,o=rwx", "210",},
        {"u=rx,g=rw,o=rw", "211",},
        {"u=rx,g=rw,o=rx", "212",},
        {"u=rx,g=rw,o=r", "213",},
        {"u=rx,g=rw,o=wx", "214",},
        {"u=rx,g=rw,o=w", "215",},
        {"u=rx,g=rw,o=x", "216",},
        {"u=rx,g=rw,o=", "217",},
        {"u=rx,g=rx,o=rwx", "220",},
        {"u=rx,g=rx,o=rw", "221",},
        {"u=rx,g=rx,o=rx", "222",},
        {"u=rx,g=rx,o=r", "223",},
        {"u=rx,g=rx,o=wx", "224",},
        {"u=rx,g=rx,o=w", "225",},
        {"u=rx,g=rx,o=x", "226",},
        {"u=rx,g=rx,o=", "227",},
        {"u=rx,g=r,o=rwx", "230",},
        {"u=rx,g=r,o=rw", "231",},
        {"u=rx,g=r,o=rx", "232",},
        {"u=rx,g=r,o=r", "233",},
        {"u=rx,g=r,o=wx", "234",},
        {"u=rx,g=r,o=w", "235",},
        {"u=rx,g=r,o=x", "236",},
        {"u=rx,g=r,o=", "237",},
        {"u=rx,g=wx,o=rwx", "240",},
        {"u=rx,g=wx,o=rw", "241",},
        {"u=rx,g=wx,o=rx", "242",},
        {"u=rx,g=wx,o=r", "243",},
        {"u=rx,g=wx,o=wx", "244",},
        {"u=rx,g=wx,o=w", "245",},
        {"u=rx,g=wx,o=x", "246",},
        {"u=rx,g=wx,o=", "247",},
        {"u=rx,g=w,o=rwx", "250",},
        {"u=rx,g=w,o=rw", "251",},
        {"u=rx,g=w,o=rx", "252",},
        {"u=rx,g=w,o=r", "253",},
        {"u=rx,g=w,o=wx", "254",},
        {"u=rx,g=w,o=w", "255",},
        {"u=rx,g=w,o=x", "256",},
        {"u=rx,g=w,o=", "257",},
        {"u=rx,g=x,o=rwx", "260",},
        {"u=rx,g=x,o=rw", "261",},
        {"u=rx,g=x,o=rx", "262",},
        {"u=rx,g=x,o=r", "263",},
        {"u=rx,g=x,o=wx", "264",},
        {"u=rx,g=x,o=w", "265",},
        {"u=rx,g=x,o=x", "266",},
        {"u=rx,g=x,o=", "267",},
        {"u=rx,g=,o=rwx", "270",},
        {"u=rx,g=,o=rw", "271",},
        {"u=rx,g=,o=rx", "272",},
        {"u=rx,g=,o=r", "273",},
        {"u=rx,g=,o=wx", "274",},
        {"u=rx,g=,o=w", "275",},
        {"u=rx,g=,o=x", "276",},
        {"u=rx,g=,o=", "277",},
        {"u=r,g=rwx,o=rwx", "300",},
        {"u=r,g=rwx,o=rw", "301",},
        {"u=r,g=rwx,o=rx", "302",},
        {"u=r,g=rwx,o=r", "303",},
        {"u=r,g=rwx,o=wx", "304",},
        {"u=r,g=rwx,o=w", "305",},
        {"u=r,g=rwx,o=x", "306",},
        {"u=r,g=rwx,o=", "307",},
        {"u=r,g=rw,o=rwx", "310",},
        {"u=r,g=rw,o=rw", "311",},
        {"u=r,g=rw,o=rx", "312",},
        {"u=r,g=rw,o=r", "313",},
        {"u=r,g=rw,o=wx", "314",},
        {"u=r,g=rw,o=w", "315",},
        {"u=r,g=rw,o=x", "316",},
        {"u=r,g=rw,o=", "317",},
        {"u=r,g=rx,o=rwx", "320",},
        {"u=r,g=rx,o=rw", "321",},
        {"u=r,g=rx,o=rx", "322",},
        {"u=r,g=rx,o=r", "323",},
        {"u=r,g=rx,o=wx", "324",},
        {"u=r,g=rx,o=w", "325",},
        {"u=r,g=rx,o=x", "326",},
        {"u=r,g=rx,o=", "327",},
        {"u=r,g=r,o=rwx", "330",},
        {"u=r,g=r,o=rw", "331",},
        {"u=r,g=r,o=rx", "332",},
        {"u=r,g=r,o=r", "333",},
        {"u=r,g=r,o=wx", "334",},
        {"u=r,g=r,o=w", "335",},
        {"u=r,g=r,o=x", "336",},
        {"u=r,g=r,o=", "337",},
        {"u=r,g=wx,o=rwx", "340",},
        {"u=r,g=wx,o=rw", "341",},
        {"u=r,g=wx,o=rx", "342",},
        {"u=r,g=wx,o=r", "343",},
        {"u=r,g=wx,o=wx", "344",},
        {"u=r,g=wx,o=w", "345",},
        {"u=r,g=wx,o=x", "346",},
        {"u=r,g=wx,o=", "347",},
        {"u=r,g=w,o=rwx", "350",},
        {"u=r,g=w,o=rw", "351",},
        {"u=r,g=w,o=rx", "352",},
        {"u=r,g=w,o=r", "353",},
        {"u=r,g=w,o=wx", "354",},
        {"u=r,g=w,o=w", "355",},
        {"u=r,g=w,o=x", "356",},
        {"u=r,g=w,o=", "357",},
        {"u=r,g=x,o=rwx", "360",},
        {"u=r,g=x,o=rw", "361",},
        {"u=r,g=x,o=rx", "362",},
        {"u=r,g=x,o=r", "363",},
        {"u=r,g=x,o=wx", "364",},
        {"u=r,g=x,o=w", "365",},
        {"u=r,g=x,o=x", "366",},
        {"u=r,g=x,o=", "367",},
        {"u=r,g=,o=rwx", "370",},
        {"u=r,g=,o=rw", "371",},
        {"u=r,g=,o=rx", "372",},
        {"u=r,g=,o=r", "373",},
        {"u=r,g=,o=wx", "374",},
        {"u=r,g=,o=w", "375",},
        {"u=r,g=,o=x", "376",},
        {"u=r,g=,o=", "377",},
        {"u=wx,g=rwx,o=rwx", "400",},
        {"u=wx,g=rwx,o=rw", "401",},
        {"u=wx,g=rwx,o=rx", "402",},
        {"u=wx,g=rwx,o=r", "403",},
        {"u=wx,g=rwx,o=wx", "404",},
        {"u=wx,g=rwx,o=w", "405",},
        {"u=wx,g=rwx,o=x", "406",},
        {"u=wx,g=rwx,o=", "407",},
        {"u=wx,g=rw,o=rwx", "410",},
        {"u=wx,g=rw,o=rw", "411",},
        {"u=wx,g=rw,o=rx", "412",},
        {"u=wx,g=rw,o=r", "413",},
        {"u=wx,g=rw,o=wx", "414",},
        {"u=wx,g=rw,o=w", "415",},
        {"u=wx,g=rw,o=x", "416",},
        {"u=wx,g=rw,o=", "417",},
        {"u=wx,g=rx,o=rwx", "420",},
        {"u=wx,g=rx,o=rw", "421",},
        {"u=wx,g=rx,o=rx", "422",},
        {"u=wx,g=rx,o=r", "423",},
        {"u=wx,g=rx,o=wx", "424",},
        {"u=wx,g=rx,o=w", "425",},
        {"u=wx,g=rx,o=x", "426",},
        {"u=wx,g=rx,o=", "427",},
        {"u=wx,g=r,o=rwx", "430",},
        {"u=wx,g=r,o=rw", "431",},
        {"u=wx,g=r,o=rx", "432",},
        {"u=wx,g=r,o=r", "433",},
        {"u=wx,g=r,o=wx", "434",},
        {"u=wx,g=r,o=w", "435",},
        {"u=wx,g=r,o=x", "436",},
        {"u=wx,g=r,o=", "437",},
        {"u=wx,g=wx,o=rwx", "440",},
        {"u=wx,g=wx,o=rw", "441",},
        {"u=wx,g=wx,o=rx", "442",},
        {"u=wx,g=wx,o=r", "443",},
        {"u=wx,g=wx,o=wx", "444",},
        {"u=wx,g=wx,o=w", "445",},
        {"u=wx,g=wx,o=x", "446",},
        {"u=wx,g=wx,o=", "447",},
        {"u=wx,g=w,o=rwx", "450",},
        {"u=wx,g=w,o=rw", "451",},
        {"u=wx,g=w,o=rx", "452",},
        {"u=wx,g=w,o=r", "453",},
        {"u=wx,g=w,o=wx", "454",},
        {"u=wx,g=w,o=w", "455",},
        {"u=wx,g=w,o=x", "456",},
        {"u=wx,g=w,o=", "457",},
        {"u=wx,g=x,o=rwx", "460",},
        {"u=wx,g=x,o=rw", "461",},
        {"u=wx,g=x,o=rx", "462",},
        {"u=wx,g=x,o=r", "463",},
        {"u=wx,g=x,o=wx", "464",},
        {"u=wx,g=x,o=w", "465",},
        {"u=wx,g=x,o=x", "466",},
        {"u=wx,g=x,o=", "467",},
        {"u=wx,g=,o=rwx", "470",},
        {"u=wx,g=,o=rw", "471",},
        {"u=wx,g=,o=rx", "472",},
        {"u=wx,g=,o=r", "473",},
        {"u=wx,g=,o=wx", "474",},
        {"u=wx,g=,o=w", "475",},
        {"u=wx,g=,o=x", "476",},
        {"u=wx,g=,o=", "477",},
        {"u=w,g=rwx,o=rwx", "500",},
        {"u=w,g=rwx,o=rw", "501",},
        {"u=w,g=rwx,o=rx", "502",},
        {"u=w,g=rwx,o=r", "503",},
        {"u=w,g=rwx,o=wx", "504",},
        {"u=w,g=rwx,o=w", "505",},
        {"u=w,g=rwx,o=x", "506",},
        {"u=w,g=rwx,o=", "507",},
        {"u=w,g=rw,o=rwx", "510",},
        {"u=w,g=rw,o=rw", "511",},
        {"u=w,g=rw,o=rx", "512",},
        {"u=w,g=rw,o=r", "513",},
        {"u=w,g=rw,o=wx", "514",},
        {"u=w,g=rw,o=w", "515",},
        {"u=w,g=rw,o=x", "516",},
        {"u=w,g=rw,o=", "517",},
        {"u=w,g=rx,o=rwx", "520",},
        {"u=w,g=rx,o=rw", "521",},
        {"u=w,g=rx,o=rx", "522",},
        {"u=w,g=rx,o=r", "523",},
        {"u=w,g=rx,o=wx", "524",},
        {"u=w,g=rx,o=w", "525",},
        {"u=w,g=rx,o=x", "526",},
        {"u=w,g=rx,o=", "527",},
        {"u=w,g=r,o=rwx", "530",},
        {"u=w,g=r,o=rw", "531",},
        {"u=w,g=r,o=rx", "532",},
        {"u=w,g=r,o=r", "533",},
        {"u=w,g=r,o=wx", "534",},
        {"u=w,g=r,o=w", "535",},
        {"u=w,g=r,o=x", "536",},
        {"u=w,g=r,o=", "537",},
        {"u=w,g=wx,o=rwx", "540",},
        {"u=w,g=wx,o=rw", "541",},
        {"u=w,g=wx,o=rx", "542",},
        {"u=w,g=wx,o=r", "543",},
        {"u=w,g=wx,o=wx", "544",},
        {"u=w,g=wx,o=w", "545",},
        {"u=w,g=wx,o=x", "546",},
        {"u=w,g=wx,o=", "547",},
        {"u=w,g=w,o=rwx", "550",},
        {"u=w,g=w,o=rw", "551",},
        {"u=w,g=w,o=rx", "552",},
        {"u=w,g=w,o=r", "553",},
        {"u=w,g=w,o=wx", "554",},
        {"u=w,g=w,o=w", "555",},
        {"u=w,g=w,o=x", "556",},
        {"u=w,g=w,o=", "557",},
        {"u=w,g=x,o=rwx", "560",},
        {"u=w,g=x,o=rw", "561",},
        {"u=w,g=x,o=rx", "562",},
        {"u=w,g=x,o=r", "563",},
        {"u=w,g=x,o=wx", "564",},
        {"u=w,g=x,o=w", "565",},
        {"u=w,g=x,o=x", "566",},
        {"u=w,g=x,o=", "567",},
        {"u=w,g=,o=rwx", "570",},
        {"u=w,g=,o=rw", "571",},
        {"u=w,g=,o=rx", "572",},
        {"u=w,g=,o=r", "573",},
        {"u=w,g=,o=wx", "574",},
        {"u=w,g=,o=w", "575",},
        {"u=w,g=,o=x", "576",},
        {"u=w,g=,o=", "577",},
        {"u=x,g=rwx,o=rwx", "600",},
        {"u=x,g=rwx,o=rw", "601",},
        {"u=x,g=rwx,o=rx", "602",},
        {"u=x,g=rwx,o=r", "603",},
        {"u=x,g=rwx,o=wx", "604",},
        {"u=x,g=rwx,o=w", "605",},
        {"u=x,g=rwx,o=x", "606",},
        {"u=x,g=rwx,o=", "607",},
        {"u=x,g=rw,o=rwx", "610",},
        {"u=x,g=rw,o=rw", "611",},
        {"u=x,g=rw,o=rx", "612",},
        {"u=x,g=rw,o=r", "613",},
        {"u=x,g=rw,o=wx", "614",},
        {"u=x,g=rw,o=w", "615",},
        {"u=x,g=rw,o=x", "616",},
        {"u=x,g=rw,o=", "617",},
        {"u=x,g=rx,o=rwx", "620",},
        {"u=x,g=rx,o=rw", "621",},
        {"u=x,g=rx,o=rx", "622",},
        {"u=x,g=rx,o=r", "623",},
        {"u=x,g=rx,o=wx", "624",},
        {"u=x,g=rx,o=w", "625",},
        {"u=x,g=rx,o=x", "626",},
        {"u=x,g=rx,o=", "627",},
        {"u=x,g=r,o=rwx", "630",},
        {"u=x,g=r,o=rw", "631",},
        {"u=x,g=r,o=rx", "632",},
        {"u=x,g=r,o=r", "633",},
        {"u=x,g=r,o=wx", "634",},
        {"u=x,g=r,o=w", "635",},
        {"u=x,g=r,o=x", "636",},
        {"u=x,g=r,o=", "637",},
        {"u=x,g=wx,o=rwx", "640",},
        {"u=x,g=wx,o=rw", "641",},
        {"u=x,g=wx,o=rx", "642",},
        {"u=x,g=wx,o=r", "643",},
        {"u=x,g=wx,o=wx", "644",},
        {"u=x,g=wx,o=w", "645",},
        {"u=x,g=wx,o=x", "646",},
        {"u=x,g=wx,o=", "647",},
        {"u=x,g=w,o=rwx", "650",},
        {"u=x,g=w,o=rw", "651",},
        {"u=x,g=w,o=rx", "652",},
        {"u=x,g=w,o=r", "653",},
        {"u=x,g=w,o=wx", "654",},
        {"u=x,g=w,o=w", "655",},
        {"u=x,g=w,o=x", "656",},
        {"u=x,g=w,o=", "657",},
        {"u=x,g=x,o=rwx", "660",},
        {"u=x,g=x,o=rw", "661",},
        {"u=x,g=x,o=rx", "662",},
        {"u=x,g=x,o=r", "663",},
        {"u=x,g=x,o=wx", "664",},
        {"u=x,g=x,o=w", "665",},
        {"u=x,g=x,o=x", "666",},
        {"u=x,g=x,o=", "667",},
        {"u=x,g=,o=rwx", "670",},
        {"u=x,g=,o=rw", "671",},
        {"u=x,g=,o=rx", "672",},
        {"u=x,g=,o=r", "673",},
        {"u=x,g=,o=wx", "674",},
        {"u=x,g=,o=w", "675",},
        {"u=x,g=,o=x", "676",},
        {"u=x,g=,o=", "677",},
        {"u=,g=rwx,o=rwx", "700",},
        {"u=,g=rwx,o=rw", "701",},
        {"u=,g=rwx,o=rx", "702",},
        {"u=,g=rwx,o=r", "703",},
        {"u=,g=rwx,o=wx", "704",},
        {"u=,g=rwx,o=w", "705",},
        {"u=,g=rwx,o=x", "706",},
        {"u=,g=rwx,o=", "707",},
        {"u=,g=rw,o=rwx", "710",},
        {"u=,g=rw,o=rw", "711",},
        {"u=,g=rw,o=rx", "712",},
        {"u=,g=rw,o=r", "713",},
        {"u=,g=rw,o=wx", "714",},
        {"u=,g=rw,o=w", "715",},
        {"u=,g=rw,o=x", "716",},
        {"u=,g=rw,o=", "717",},
        {"u=,g=rx,o=rwx", "720",},
        {"u=,g=rx,o=rw", "721",},
        {"u=,g=rx,o=rx", "722",},
        {"u=,g=rx,o=r", "723",},
        {"u=,g=rx,o=wx", "724",},
        {"u=,g=rx,o=w", "725",},
        {"u=,g=rx,o=x", "726",},
        {"u=,g=rx,o=", "727",},
        {"u=,g=r,o=rwx", "730",},
        {"u=,g=r,o=rw", "731",},
        {"u=,g=r,o=rx", "732",},
        {"u=,g=r,o=r", "733",},
        {"u=,g=r,o=wx", "734",},
        {"u=,g=r,o=w", "735",},
        {"u=,g=r,o=x", "736",},
        {"u=,g=r,o=", "737",},
        {"u=,g=wx,o=rwx", "740",},
        {"u=,g=wx,o=rw", "741",},
        {"u=,g=wx,o=rx", "742",},
        {"u=,g=wx,o=r", "743",},
        {"u=,g=wx,o=wx", "744",},
        {"u=,g=wx,o=w", "745",},
        {"u=,g=wx,o=x", "746",},
        {"u=,g=wx,o=", "747",},
        {"u=,g=w,o=rwx", "750",},
        {"u=,g=w,o=rw", "751",},
        {"u=,g=w,o=rx", "752",},
        {"u=,g=w,o=r", "753",},
        {"u=,g=w,o=wx", "754",},
        {"u=,g=w,o=w", "755",},
        {"u=,g=w,o=x", "756",},
        {"u=,g=w,o=", "757",},
        {"u=,g=x,o=rwx", "760",},
        {"u=,g=x,o=rw", "761",},
        {"u=,g=x,o=rx", "762",},
        {"u=,g=x,o=r", "763",},
        {"u=,g=x,o=wx", "764",},
        {"u=,g=x,o=w", "765",},
        {"u=,g=x,o=x", "766",},
        {"u=,g=x,o=", "767",},
        {"u=,g=,o=rwx", "770",},
        {"u=,g=,o=rw", "771",},
        {"u=,g=,o=rx", "772",},
        {"u=,g=,o=r", "773",},
        {"u=,g=,o=wx", "774",},
        {"u=,g=,o=w", "775",},
        {"u=,g=,o=x", "776",},
        {"u=,g=,o=", "777"}
    };
    
    for(int i = 0; i < symbolic.length; i += 2) {
      conf.set(FsPermission.UMASK_LABEL, symbolic[i][0]);
      short val = Short.valueOf(symbolic[i][1], 8);
      assertEquals(val, FsPermission.getUMask(conf).toShort());
    }
  }

  public void testBadUmasks() {
    Configuration conf = new Configuration();
    
    for(String b : new String [] {"1777", "22", "99", "foo", ""}) {
      conf.set(FsPermission.UMASK_LABEL, b); 
      try {
        FsPermission.getUMask(conf);
        fail("Shouldn't have been able to parse bad umask");
      } catch(IllegalArgumentException iae) {
        assertEquals(iae.getMessage(), b);
      }
    }
  }
  
  // Ensure that when the deprecated decimal umask key is used, it is correctly
  // parsed as such and converted correctly to an FsPermission value
  public void testDeprecatedUmask() {
    Configuration conf = new Configuration();
    conf.set(FsPermission.DEPRECATED_UMASK_LABEL, "302"); // 302 = 0456
    FsPermission umask = FsPermission.getUMask(conf);

    assertEquals(0456, umask.toShort());
  }
}
