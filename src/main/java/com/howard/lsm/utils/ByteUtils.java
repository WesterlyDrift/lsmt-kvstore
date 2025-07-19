package com.howard.lsm.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * 字节工具类
 *
 * ByteUtils是我们存储引擎的"瑞士军刀"，提供各种字节操作的实用方法。
 * 在存储引擎中，我们经常需要在不同的数据类型之间转换，ByteUtils
 * 让这些操作变得简单、安全且高效。
 *
 * 为什么需要字节工具类？
 * 1. 类型转换：在基本类型和字节数组之间转换
 * 2. 数据比较：高效的字节数组比较
 * 3. 编码处理：统一的字符串编码方式
 * 4. 内存管理：减少不必要的内存分配
 */
public class ByteUtils {

    /**
     * 将整数转换为字节数组（大端序）
     *
     * 大端序（Big-Endian）是网络字节序，也是Java的默认字节序。
     * 使用统一的字节序确保数据在不同平台间的兼容性。
     */
    public static byte[] intToBytes(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    /**
     * 将字节数组转换为整数（大端序）
     */
    public static int bytesToInt(byte[] bytes) {
        if (bytes.length != 4) {
            throw new IllegalArgumentException("Array length must be 4 for int conversion");
        }
        return ByteBuffer.wrap(bytes).getInt();
    }

    /**
     * 将长整数转换为字节数组（大端序）
     */
    public static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    /**
     * 将字节数组转换为长整数（大端序）
     */
    public static long bytesToLong(byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalArgumentException("Array length must be 8 for long conversion");
        }
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * 将字符串转换为字节数组（UTF-8编码）
     *
     * 统一使用UTF-8编码确保国际化支持和跨平台兼容性
     */
    public static byte[] stringToBytes(String str) {
        if (str == null) {
            return new byte[0];
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * 将字节数组转换为字符串（UTF-8解码）
     */
    public static String bytesToString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 比较两个字节数组
     *
     * 这个方法提供了字典序比较，这对于LSM-Tree中的键排序至关重要。
     * 我们不能简单使用Arrays.equals()，因为我们需要确定大小关系。
     *
     * @param a 第一个字节数组
     * @param b 第二个字节数组
     * @return 负数表示a<b，0表示a=b，正数表示a>b
     */
    public static int compare(byte[] a, byte[] b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }

        int minLength = Math.min(a.length, b.length);
        for (int i = 0; i < minLength; i++) {
            int result = Byte.compareUnsigned(a[i], b[i]);
            if (result != 0) {
                return result;
            }
        }

        // 前缀相同，比较长度
        return Integer.compare(a.length, b.length);
    }

    /**
     * 检查两个字节数组是否相等
     */
    public static boolean equals(byte[] a, byte[] b) {
        return compare(a, b) == 0;
    }

    /**
     * 连接多个字节数组
     *
     * 这个方法在构建复合键或合并数据时很有用。
     * 我们预先计算总长度以避免多次数组扩容。
     */
    public static byte[] concat(byte[]... arrays) {
        if (arrays == null || arrays.length == 0) {
            return new byte[0];
        }

        // 计算总长度
        int totalLength = 0;
        for (byte[] array : arrays) {
            if (array != null) {
                totalLength += array.length;
            }
        }

        // 合并数组
        byte[] result = new byte[totalLength];
        int offset = 0;
        for (byte[] array : arrays) {
            if (array != null) {
                System.arraycopy(array, 0, result, offset, array.length);
                offset += array.length;
            }
        }

        return result;
    }

    /**
     * 复制字节数组的子段
     */
    public static byte[] slice(byte[] source, int start, int length) {
        if (source == null) {
            throw new IllegalArgumentException("Source array cannot be null");
        }
        if (start < 0 || start > source.length) {
            throw new IndexOutOfBoundsException("Start index out of bounds");
        }
        if (length < 0 || start + length > source.length) {
            throw new IndexOutOfBoundsException("Length out of bounds");
        }

        byte[] result = new byte[length];
        System.arraycopy(source, start, result, 0, length);
        return result;
    }

    /**
     * 计算字节数组的哈希值
     *
     * 使用FNV-1a哈希算法，它对于短字符串有很好的分布特性，
     * 适合用作哈希表的键。
     */
    public static int hash(byte[] data) {
        if (data == null || data.length == 0) {
            return 0;
        }

        int hash = 0x811c9dc5; // FNV-1a 32-bit offset basis
        for (byte b : data) {
            hash ^= (b & 0xff);
            hash *= 0x01000193; // FNV-1a 32-bit prime
        }
        return hash;
    }

    /**
     * 将字节数组转换为十六进制字符串
     *
     * 这个方法主要用于调试和日志记录，让二进制数据变得可读。
     */
    public static String toHexString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }

        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    /**
     * 将十六进制字符串转换为字节数组
     */
    public static byte[] fromHexString(String hex) {
        if (hex == null || hex.length() == 0) {
            return new byte[0];
        }
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("Hex string length must be even");
        }

        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            int index = i * 2;
            result[i] = (byte) Integer.parseInt(hex.substring(index, index + 2), 16);
        }
        return result;
    }

    /**
     * 检查字节数组是否为空或null
     */
    public static boolean isEmpty(byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    /**
     * 安全地获取字节数组的长度
     */
    public static int length(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }
}