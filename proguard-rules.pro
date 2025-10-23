# 基本配置
-dontusemixedcaseclassnames
-dontskipnonpubliclibraryclasses

# 保留栈映射帧
-keepattributes StackMapTable

# 禁用压缩和优化（调试用）
-dontshrink
-dontoptimize

# 指定字典文件
-obfuscationdictionary dictionary.txt
-classobfuscationdictionary dictionary.txt
-packageobfuscationdictionary dictionary.txt

# 保留 MagicZip 类及其 generate 方法
-keep public class net.momirealms.craftengine.core.pack.obfuscation.ObfH {
    public void generate();
}

# 日志输出
-verbose
-printmapping mapping.txt