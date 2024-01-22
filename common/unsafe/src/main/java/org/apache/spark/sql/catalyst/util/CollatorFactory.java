package org.apache.spark.sql.catalyst.util;

import com.ibm.icu.util.ULocale;

import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.Comparator;
import com.ibm.icu.text.Collator;

public class CollatorFactory {
  public class CollatorInfo {
    public String collationName;
    public Collator collator;
    public Comparator<UTF8String> comparator;
    public String version;
    public Function<UTF8String, Integer> hashFunction;
    public BiFunction<UTF8String, UTF8String, Boolean> equalsFunction;

    public CollatorInfo(String collationName, Collator collator, Comparator<UTF8String> comparator, String version, Function<UTF8String, Integer> hashFunction) {
      this.collationName = collationName;
      this.collator = collator;
      this.comparator = comparator;
      this.version = version;
      this.hashFunction = hashFunction;
      this.equalsFunction = (s1, s2) -> this.comparator.compare(s1, s2) == 0;
    }

    public CollatorInfo(String collationName, Collator collator, String version) {
      this.collationName = collationName;
      this.collator = collator;
      this.comparator = (s1, s2) -> this.collator.compare(s1.toString(), s2.toString());
      this.version = version;
      this.hashFunction = s -> this.collator.getCollationKey(s.toString()).hashCode();
      this.equalsFunction = (s1, s2) -> this.comparator.compare(s1, s2) == 0;
    }
  }

  private final CollatorInfo[] collatorTable;

  private static Collator CollatorFromCollationName(String collationName) {
    // Expected format is <locale>-<ci/cs>-<ai/as>
    var split = collationName.split("-");
    if (split.length != 3) {
      throw new IllegalArgumentException("Invalid collation name: " + collationName);
    }

    Collator collator = Collator.getInstance(java.util.Locale.forLanguageTag(split[0]));

    if (split[1].equalsIgnoreCase("ci") && split[2].equalsIgnoreCase("ai")) {
        collator.setStrength(Collator.PRIMARY);
        } else if (split[1].equalsIgnoreCase("ci") && split[2].equalsIgnoreCase("as")) {
        collator.setStrength(Collator.SECONDARY);
        } else if (split[1].equalsIgnoreCase("cs") && split[2].equalsIgnoreCase("as")) {
        collator.setStrength(Collator.TERTIARY);
        } else {
        throw new IllegalArgumentException("Invalid collation name: " + collationName);
    }

    return collator;
  }

  private CollatorFactory() {
    collatorTable = new CollatorInfo[10];

    // Binary comparison. This is the default collation.
    // No custom comparators will be used for this collation.
    // Instead, we rely on byte for byte comparison.
    collatorTable[0] = new CollatorInfo("UCS_BASIC", null, UTF8String::binaryCompare, "1.0", UTF8String::binaryHash);
    collatorTable[0].equalsFunction = UTF8String::binaryEquals;

    // First do lower case conversion, then do binary comparison.
    collatorTable[1] = new CollatorInfo("UCS_BASIC_LCASE", null, (s1, s2) -> s1.toLowerCase().binaryCompare(s2.toLowerCase()), "1.0", (s) -> s.toLowerCase().binaryHash());

    // UNICODE case sensitive comparison (ROOT locale, in ICU).
    collatorTable[2] = new CollatorInfo("UNICODE", Collator.getInstance(ULocale.ROOT), "153.120.0.0");


    // UNICODE case insensitive comparison (ROOT locale, in ICU).
    collatorTable[3] = new CollatorInfo("UNICODE", Collator.getInstance(ULocale.ROOT), "153.120.0.0");
    collatorTable[3].collator.setStrength(Collator.SECONDARY);

    // TODO: Create all the other collators here. e.g. for Serbian. We can decide to do this lazily as well.
    // NOTE: Measured impact of installing all available collators is ~10MBs
    collatorTable[4] = new CollatorInfo("SR-CI-AI", CollatorFromCollationName("sr-ci-ai"), "153.120.0.0");
    collatorTable[5] = new CollatorInfo("SR-CI-AS", CollatorFromCollationName("sr-ci-as"), "153.120.0.0");
    collatorTable[6] = new CollatorInfo("SR-CS-AS", CollatorFromCollationName("sr-cs-as"), "153.120.0.0");

    // German
    collatorTable[7] = new CollatorInfo("DE-CI-AI", CollatorFromCollationName("De-ci-ai"), "153.120.0.0");
    collatorTable[8] = new CollatorInfo("DE-CI-AS", CollatorFromCollationName("De-ci-as"), "153.120.0.0");
    collatorTable[9] = new CollatorInfo("DE-CS-AS", CollatorFromCollationName("De-cs-as"), "153.120.0.0");
  }

  public int collationNameToId(String collationName) {
    // TODO: Build map here.
    for (int i = 0; i < collatorTable.length; i++) {
      if (collatorTable[i].collationName.equalsIgnoreCase(collationName)) {
        return i;
      }
    }

    throw new IllegalArgumentException("Invalid collation name: " + collationName);
  }

  // TODO: This probably should not be a singleton.
  private static final CollatorFactory instance = new CollatorFactory();

  public static CollatorFactory getInstance() {
    return instance;
  }

  public static CollatorInfo getInfoForId(int id) {
    return instance.collatorTable[id];
  }

  public static String DEFAULT_COLLATION_NAME = "UCS_BASIC";
}