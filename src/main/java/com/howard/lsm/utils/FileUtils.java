package com.howard.lsm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * 文件工具类
 *
 * FileUtils是我们存储引擎的"文件管家"，负责处理所有与文件系统相关的操作。
 * 它封装了复杂的文件操作，提供简单、安全、跨平台的文件管理功能。
 *
 * 文件操作的挑战：
 * 1. 跨平台兼容性：不同操作系统的文件系统差异
 * 2. 错误处理：I/O操作可能失败，需要优雅的错误处理
 * 3. 资源管理：确保文件句柄正确关闭
 * 4. 原子性操作：确保文件操作的原子性，避免数据不一致
 */
public class FileUtils {
    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * 确保目录存在
     *
     * 这个方法会创建指定路径的目录，包括所有必要的父目录。
     * 如果目录已存在，则不做任何操作。这是一个幂等操作。
     */
    public static void ensureDirectoryExists(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            try {
                Files.createDirectories(directory);
                logger.debug("Created directory: {}", directory);
            } catch (IOException e) {
                logger.error("Failed to create directory: {}", directory, e);
                throw e;
            }
        }
    }

    /**
     * 安全地删除文件
     *
     * 删除文件时需要处理文件不存在的情况。这个方法提供了
     * 安全的删除操作，不会因为文件不存在而抛出异常。
     */
    public static boolean deleteFileIfExists(Path file) {
        try {
            return Files.deleteIfExists(file);
        } catch (IOException e) {
            logger.warn("Failed to delete file: {}", file, e);
            return false;
        }
    }

    /**
     * 递归删除目录
     *
     * Java的标准API不能直接删除非空目录，这个方法提供了
     * 递归删除功能，会先删除目录中的所有文件和子目录。
     */
    public static void deleteDirectoryRecursively(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });

        logger.debug("Recursively deleted directory: {}", directory);
    }

    /**
     * 原子性文件写入
     *
     * 这个方法实现了原子性文件写入：先写入临时文件，写入成功后
     * 再重命名为目标文件。这确保了其他进程不会看到部分写入的文件。
     */
    public static void writeFileAtomically(Path targetFile, byte[] data) throws IOException {
        Path tempFile = targetFile.resolveSibling(targetFile.getFileName() + ".tmp");

        try {
            // 写入临时文件
            Files.write(tempFile, data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

            // 原子性重命名
            Files.move(tempFile, targetFile, StandardCopyOption.ATOMIC_MOVE);

            logger.debug("Atomically wrote file: {}", targetFile);

        } catch (IOException e) {
            // 清理临时文件
            deleteFileIfExists(tempFile);
            throw e;
        }
    }

    /**
     * 安全地读取文件
     *
     * 这个方法提供了安全的文件读取，包括文件存在性检查和
     * 适当的错误处理。
     */
    public static byte[] readFileIfExists(Path file) throws IOException {
        if (!Files.exists(file)) {
            return null;
        }

        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            logger.error("Failed to read file: {}", file, e);
            throw e;
        }
    }

    /**
     * 列出目录中的所有文件
     *
     * 返回指定目录中所有常规文件的路径列表。
     * 这个方法过滤掉了目录和特殊文件。
     */
    public static List<Path> listFiles(Path directory) throws IOException {
        List<Path> files = new ArrayList<>();

        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            return files;
        }

        try (Stream<Path> stream = Files.list(directory)) {
            stream.filter(Files::isRegularFile)
                    .forEach(files::add);
        }

        return files;
    }

    /**
     * 列出目录中匹配模式的文件
     *
     * 使用glob模式匹配文件名。例如："*.dat" 匹配所有.dat文件。
     */
    public static List<Path> listFiles(Path directory, String pattern) throws IOException {
        List<Path> files = new ArrayList<>();

        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            return files;
        }

        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);

        try (Stream<Path> stream = Files.list(directory)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> matcher.matches(path.getFileName()))
                    .forEach(files::add);
        }

        return files;
    }

    /**
     * 获取文件大小
     *
     * 安全地获取文件大小，如果文件不存在则返回0。
     */
    public static long getFileSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            logger.warn("Failed to get size of file: {}", file, e);
            return 0;
        }
    }

    /**
     * 计算目录大小
     *
     * 递归计算目录及其所有子目录和文件的总大小。
     */
    public static long getDirectorySize(Path directory) throws IOException {
        if (!Files.exists(directory) || !Files.isDirectory(directory)) {
            return 0;
        }

        final long[] size = {0};

        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                size[0] += attrs.size();
                return FileVisitResult.CONTINUE;
            }
        });

        return size[0];
    }

    /**
     * 移动文件
     *
     * 安全地移动文件，包括跨文件系统的移动。
     * 如果目标文件已存在，会被覆盖。
     */
    public static void moveFile(Path source, Path target) throws IOException {
        ensureDirectoryExists(target.getParent());

        try {
            Files.move(source, target,
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            logger.debug("Moved file from {} to {}", source, target);
        } catch (AtomicMoveNotSupportedException e) {
            // 回退到非原子移动
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            logger.debug("Moved file from {} to {} (non-atomic)", source, target);
        }
    }

    /**
     * 复制文件
     *
     * 安全地复制文件，保留文件属性。
     */
    public static void copyFile(Path source, Path target) throws IOException {
        ensureDirectoryExists(target.getParent());

        Files.copy(source, target,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.COPY_ATTRIBUTES);

        logger.debug("Copied file from {} to {}", source, target);
    }

    /**
     * 创建临时文件
     *
     * 在指定目录中创建临时文件，使用指定的前缀和后缀。
     */
    public static Path createTempFile(Path directory, String prefix, String suffix) throws IOException {
        ensureDirectoryExists(directory);
        return Files.createTempFile(directory, prefix, suffix);
    }

    /**
     * 同步文件到磁盘
     *
     * 强制将文件数据写入磁盘，确保数据持久性。
     * 这对于WAL日志等关键数据特别重要。
     */
    public static void syncFile(Path file) throws IOException {
        try (java.nio.channels.FileChannel channel =
                     java.nio.channels.FileChannel.open(file, java.nio.file.StandardOpenOption.WRITE)) {
            channel.force(true);
            logger.debug("File synced to disk: {}", file);
        }
    }

    /**
     * 检查文件是否可读
     */
    public static boolean isReadable(Path file) {
        return Files.exists(file) && Files.isReadable(file);
    }

    /**
     * 检查文件是否可写
     */
    public static boolean isWritable(Path file) {
        return Files.exists(file) && Files.isWritable(file);
    }

    /**
     * 获取文件的最后修改时间
     */
    public static long getLastModifiedTime(Path file) {
        try {
            return Files.getLastModifiedTime(file).toMillis();
        } catch (IOException e) {
            logger.warn("Failed to get last modified time for file: {}", file, e);
            return 0;
        }
    }
}