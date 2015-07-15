package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.unsafe.PlatformDependent;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.apache.spark.unsafe.memory.TaskMemoryManager;

/**
 * memory location of a <k, v> pair
 */
public class UnsafeRowLocation {

  private final TaskMemoryManager memoryManager;

  /**
   * An index into the hash map's Long array
   */
  private int pos;
  /**
   * True if this location points to a position where a key is defined, false otherwise
   */
  private boolean isDefined;
  /**
   * The hashcode of the most recent key passed to caching this hashcode here allows us to
   * avoid re-hashing the key when storing a value for that key.
   */
  private int keyHashcode;

  private final MemoryLocation keyMemoryLocation = new MemoryLocation();
  private final MemoryLocation valueMemoryLocation = new MemoryLocation();
  private int keyLength;
  private int valueLength;

  public UnsafeRowLocation(TaskMemoryManager memoryManager) {
    this.memoryManager = memoryManager;
  }

  public UnsafeRowLocation with(long fullKeyAddress) {
    this.isDefined = true;
    updateAddressesAndSizes(fullKeyAddress);
    return this;
  }

  public UnsafeRowLocation with(Object page, long offsetInPage) {
    this.isDefined = true;
    updateAddressesAndSizes(page, offsetInPage);
    return this;
  }

  public UnsafeRowLocation with(int keyLength, byte[] keyArray, int valueLength,
      byte[] valueArray) {
    this.isDefined = true;
    this.keyLength = keyLength;
    keyMemoryLocation.setObjAndOffset(keyArray, PlatformDependent.BYTE_ARRAY_OFFSET);
    this.valueLength = valueLength;
    valueMemoryLocation.setObjAndOffset(valueArray, PlatformDependent.BYTE_ARRAY_OFFSET);
    return this;
  }

  public UnsafeRowLocation with(int pos, int keyHashcode, boolean isDefined, long fullKeyAddress) {
    this.pos = pos;
    this.isDefined = isDefined;
    this.keyHashcode = keyHashcode;
    if (isDefined) {
      updateAddressesAndSizes(fullKeyAddress);
    }
    return this;
  }

  public void updateAddressesAndSizes(long fullKeyAddress) {
    updateAddressesAndSizes(
        memoryManager.getPage(fullKeyAddress),
        memoryManager.getOffsetInPage(fullKeyAddress));
  }

  private void updateAddressesAndSizes(Object page, long offsetInPage) {
    long position = offsetInPage;
    keyLength = (int) PlatformDependent.UNSAFE.getLong(page, position);
    position += 8; // word used to store the key size
    keyMemoryLocation.setObjAndOffset(page, position);
    position += keyLength;
    valueLength = (int) PlatformDependent.UNSAFE.getLong(page, position);
    position += 8; // word used to store the key size
    valueMemoryLocation.setObjAndOffset(page, position);
  }

  /**
   * Returns true if the key is defined at this position, and false otherwise.
   */
  public boolean isDefined() {
    return isDefined;
  }

  /**
   * Set whether the key is defined.
   */
  public void setDefined(boolean isDefined) {
    this.isDefined = isDefined;
  }

  /**
   * Returns the hashcode of the key.
   */
  public long getKeyHashcode() {
    return this.keyHashcode;
  }

  /**
   * Set the hashcode of the key.
   */
  public void setKeyHashcode(int keyHashcode) {
    this.keyHashcode = keyHashcode;
  }

  /**
   * Set an index into the hash map's Long array.
   */
  public void setPos(int pos) {
    this.pos = pos;
  }

  /**
   * Returns the index into the hash map's Long array.
   */
  public int getPos() {
    return this.pos;
  }

  /**
   * Returns the address of the key defined at this position.
   * This points to the first byte of the key data.
   * Unspecified behavior if the key is not defined.
   * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
   */
  public MemoryLocation getKeyAddress() {
    assert (isDefined);
    return keyMemoryLocation;
  }

  /**
   * Returns the base object of the key.
   */
  public Object getKeyBaseObject() {
    assert (isDefined);
    return keyMemoryLocation.getBaseObject();
  }

  /**
   * Returns the base offset of the key.
   */
  public long getKeyBaseOffset() {
    assert (isDefined);
    return keyMemoryLocation.getBaseOffset();
  }

  /**
   * Returns the length of the key defined at this position.
   * Unspecified behavior if the key is not defined.
   */
  public int getKeyLength() {
    assert (isDefined);
    return keyLength;
  }

  /**
   * Returns the address of the value defined at this position.
   * This points to the first byte of the value data.
   * Unspecified behavior if the key is not defined.
   * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
   */
  public MemoryLocation getValueAddress() {
    assert (isDefined);
    return valueMemoryLocation;
  }

  /**
   * Returns the base object of the value.
   */
  public Object getValueBaseObject() {
    assert (isDefined);
    return valueMemoryLocation.getBaseObject();
  }

  /**
   * Return the base offset of the value.
   */
  public long getValueBaseOffset() {
    assert (isDefined);
    return valueMemoryLocation.getBaseOffset();
  }

  /**
   * Returns the length of the value defined at this position.
   * Unspecified behavior if the key is not defined.
   */
  public int getValueLength() {
    assert (isDefined);
    return valueLength;
  }
}
