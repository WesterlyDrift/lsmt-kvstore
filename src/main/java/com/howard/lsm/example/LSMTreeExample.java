package com.howard.lsm.example;

import com.howard.lsm.LSMTree;
import com.howard.lsm.config.LSMConfig;
import com.howard.lsm.transaction.Transaction;
import com.howard.lsm.transaction.TransactionManager;

import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

/**
 * LSM-Tree存储引擎完整示例
 *
 * 这个示例程序演示了LSM-Tree存储引擎的各种功能，
 * 包括基本操作、事务处理、性能测试等。
 */
public class LSMTreeExample {

    private static LSMTree lsmTree;
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        System.out.println("=== LSM-Tree KV存储引擎示例程序 ===");

        try {
            // 初始化存储引擎
            initializeEngine();

            // 显示主菜单
            showMainMenu();

        } catch (Exception e) {
            System.err.println("程序执行出错: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup();
        }
    }

    /**
     * 初始化存储引擎
     */
    private static void initializeEngine() throws Exception {
        System.out.println("正在初始化LSM-Tree存储引擎...");

        // 创建配置
        LSMConfig config = new LSMConfig();

        // 创建存储引擎实例
        lsmTree = new LSMTree(config);

        System.out.println("✓ 存储引擎初始化完成");
        System.out.println("  数据目录: " + config.getDataDirectory());
        System.out.println("  WAL目录: " + config.getWalDirectory());
        System.out.println("  内存表大小: " + (config.getMemTableSize() / 1024 / 1024) + "MB");
        System.out.println();
    }

    /**
     * 显示主菜单
     */
    private static void showMainMenu() throws Exception {
        while (true) {
            System.out.println("请选择操作:");
            System.out.println("1. 基本操作演示");
            System.out.println("2. 事务操作演示");
            System.out.println("3. 性能测试");
            System.out.println("4. 交互式命令行");
            System.out.println("5. 退出程序");
            System.out.print("请输入选择 (1-5): ");

            String choice = scanner.nextLine().trim();

            switch (choice) {
                case "1":
                    basicOperationsDemo();
                    break;
                case "2":
                    transactionDemo();
                    break;
                case "3":
                    performanceTest();
                    break;
                case "4":
                    interactiveMode();
                    break;
                case "5":
                    System.out.println("正在退出程序...");
                    return;
                default:
                    System.out.println("无效选择，请重新输入");
            }

            System.out.println();
        }
    }

    /**
     * 基本操作演示
     */
    private static void basicOperationsDemo() throws Exception {
        System.out.println("\n=== 基本操作演示 ===");

        // 1. 写入数据
        System.out.println("1. 写入数据...");
        lsmTree.put("user:1001", "张三,25,工程师".getBytes());
        lsmTree.put("user:1002", "李四,30,设计师".getBytes());
        lsmTree.put("user:1003", "王五,28,产品经理".getBytes());
        System.out.println("✓ 写入3条用户数据");

        // 2. 读取数据
        System.out.println("\n2. 读取数据...");
        byte[] userData = lsmTree.get("user:1001");
        if (userData != null) {
            System.out.println("user:1001 = " + new String(userData));
        }

        userData = lsmTree.get("user:1002");
        if (userData != null) {
            System.out.println("user:1002 = " + new String(userData));
        }

        // 3. 更新数据
        System.out.println("\n3. 更新数据...");
        lsmTree.put("user:1001", "张三,26,高级工程师".getBytes());
        userData = lsmTree.get("user:1001");
        System.out.println("更新后 user:1001 = " + new String(userData));

        // 4. 删除数据
        System.out.println("\n4. 删除数据...");
        lsmTree.delete("user:1003");
        userData = lsmTree.get("user:1003");
        System.out.println("删除后 user:1003 = " + (userData == null ? "null" : new String(userData)));

        // 5. 查询不存在的键
        System.out.println("\n5. 查询不存在的键...");
        userData = lsmTree.get("user:9999");
        System.out.println("user:9999 = " + (userData == null ? "null" : new String(userData)));

        System.out.println("✓ 基本操作演示完成");
    }

    /**
     * 事务操作演示
     */
    private static void transactionDemo() throws Exception {
        System.out.println("\n=== 事务操作演示 ===");

        TransactionManager txManager = lsmTree.getTransactionManager();

        // 演示1: 成功提交的事务
        System.out.println("1. 成功提交的事务...");
        Transaction tx1 = txManager.beginTransaction();
        try {
            tx1.put("account:001", "1000.00".getBytes());
            tx1.put("account:002", "2000.00".getBytes());

            System.out.println("事务中 account:001 = " + new String(tx1.get("account:001")));
            System.out.println("事务中 account:002 = " + new String(tx1.get("account:002")));

            tx1.commit();
            System.out.println("✓ 事务提交成功");

            // 验证提交后的数据
            System.out.println("提交后 account:001 = " + new String(lsmTree.get("account:001")));
            System.out.println("提交后 account:002 = " + new String(lsmTree.get("account:002")));

        } catch (Exception e) {
            tx1.rollback();
            System.out.println("✗ 事务回滚: " + e.getMessage());
        }

        // 演示2: 回滚的事务
        System.out.println("\n2. 回滚的事务...");
        Transaction tx2 = txManager.beginTransaction();
        try {
            tx2.put("account:001", "500.00".getBytes());
            tx2.put("account:003", "1500.00".getBytes());

            System.out.println("事务中 account:001 = " + new String(tx2.get("account:001")));
            System.out.println("事务中 account:003 = " + new String(tx2.get("account:003")));

            // 模拟业务错误
            throw new RuntimeException("模拟业务异常");

        } catch (Exception e) {
            tx2.rollback();
            System.out.println("✗ 事务回滚: " + e.getMessage());

            // 验证回滚后的数据
            System.out.println("回滚后 account:001 = " + new String(lsmTree.get("account:001")));
            byte[] account003 = lsmTree.get("account:003");
            System.out.println("回滚后 account:003 = " + (account003 == null ? "null" : new String(account003)));
        }

        System.out.println("✓ 事务操作演示完成");
    }

    /**
     * 性能测试
     */
    private static void performanceTest() throws Exception {
        System.out.println("\n=== 性能测试 ===");

        System.out.print("请输入测试数据量 (建议1000-50000): ");
        String input = scanner.nextLine().trim();
        int numOperations;
        try {
            numOperations = Integer.parseInt(input);
            if (numOperations <= 0 || numOperations > 100000) {
                System.out.println("数据量应在1-100000之间，使用默认值10000");
                numOperations = 10000;
            }
        } catch (NumberFormatException e) {
            System.out.println("输入无效，使用默认值10000");
            numOperations = 10000;
        }

        // 写入性能测试
        System.out.println("\n正在进行写入性能测试...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numOperations; i++) {
            String key = String.format("perf_key_%06d", i);
            String value = String.format("performance_test_value_%d_%d", i, ThreadLocalRandom.current().nextInt(1000000));
            lsmTree.put(key, value.getBytes());

            if ((i + 1) % (numOperations / 10) == 0) {
                System.out.printf("已写入 %d/%d (%.1f%%)\n", i + 1, numOperations, (i + 1) * 100.0 / numOperations);
            }
        }

        long writeTime = System.currentTimeMillis() - startTime;
        double writeQPS = numOperations * 1000.0 / writeTime;

        System.out.printf("✓ 写入完成: %d 条记录，耗时 %d ms，QPS: %.2f\n", numOperations, writeTime, writeQPS);

        // 读取性能测试
        System.out.println("\n正在进行读取性能测试...");
        startTime = System.currentTimeMillis();
        int foundCount = 0;

        for (int i = 0; i < numOperations; i++) {
            String key = String.format("perf_key_%06d", i);
            byte[] value = lsmTree.get(key);
            if (value != null) {
                foundCount++;
            }

            if ((i + 1) % (numOperations / 10) == 0) {
                System.out.printf("已读取 %d/%d (%.1f%%)\n", i + 1, numOperations, (i + 1) * 100.0 / numOperations);
            }
        }

        long readTime = System.currentTimeMillis() - startTime;
        double readQPS = numOperations * 1000.0 / readTime;

        System.out.printf("✓ 读取完成: 找到 %d/%d 条记录，耗时 %d ms，QPS: %.2f\n",
                foundCount, numOperations, readTime, readQPS);

        // 触发压缩
        System.out.println("\n触发压缩...");
        lsmTree.compact();
        System.out.println("✓ 压缩完成");

        System.out.println("✓ 性能测试完成");
    }

    /**
     * 交互式命令行模式
     */
    private static void interactiveMode() throws Exception {
        System.out.println("\n=== 交互式命令行模式 ===");
        System.out.println("支持的命令:");
        System.out.println("  put <key> <value>  - 写入键值对");
        System.out.println("  get <key>          - 读取键对应的值");
        System.out.println("  del <key>          - 删除键");
        System.out.println("  compact            - 触发压缩");
        System.out.println("  exit               - 退出交互模式");
        System.out.println();

        while (true) {
            System.out.print("lsm> ");
            String line = scanner.nextLine().trim();

            if (line.isEmpty()) {
                continue;
            }

            if (line.equals("exit")) {
                break;
            }

            String[] parts = line.split("\\s+", 3);
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "put":
                        if (parts.length < 3) {
                            System.out.println("用法: put <key> <value>");
                            break;
                        }
                        lsmTree.put(parts[1], parts[2].getBytes());
                        System.out.println("✓ 已存储");
                        break;

                    case "get":
                        if (parts.length < 2) {
                            System.out.println("用法: get <key>");
                            break;
                        }
                        byte[] value = lsmTree.get(parts[1]);
                        if (value != null) {
                            System.out.println(new String(value));
                        } else {
                            System.out.println("(null)");
                        }
                        break;

                    case "del":
                        if (parts.length < 2) {
                            System.out.println("用法: del <key>");
                            break;
                        }
                        lsmTree.delete(parts[1]);
                        System.out.println("✓ 已删除");
                        break;

                    case "compact":
                        System.out.println("正在执行压缩...");
                        lsmTree.compact();
                        System.out.println("✓ 压缩完成");
                        break;

                    default:
                        System.out.println("未知命令: " + command);
                        break;
                }
            } catch (Exception e) {
                System.out.println("执行命令时出错: " + e.getMessage());
            }
        }

        System.out.println("退出交互模式");
    }

    /**
     * 清理资源
     */
    private static void cleanup() {
        if (lsmTree != null) {
            try {
                lsmTree.close();
                System.out.println("✓ 存储引擎已关闭");
            } catch (Exception e) {
                System.err.println("关闭存储引擎时出错: " + e.getMessage());
            }
        }

        if (scanner != null) {
            scanner.close();
        }
    }
}