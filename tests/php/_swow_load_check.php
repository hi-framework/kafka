<?php

declare(strict_types=1);

/**
 * 不需要装 Swow 也能跑：验证 SwowClient 类可被 autoload，
 * 实例化时给出明确「需要 swow 扩展」错误。
 */

spl_autoload_register(function (string $cls): void {
    $base = __DIR__ . '/../../php-driver/src/';
    $path = $base . str_replace('\\', '/', $cls) . '.php';
    if (file_exists($path)) {
        require $path;
    }
});

if (! class_exists('Hi\\Kafka\\SwowClient')) {
    fwrite(STDERR, "FAIL: SwowClient class 没法 autoload\n");
    exit(1);
}
echo "✓ SwowClient 类已注册 (autoload 命中)" . PHP_EOL;

try {
    new Hi\Kafka\SwowClient('/tmp/never-used.sock');
    fwrite(STDERR, "FAIL: 没装 swow 时构造函数应该抛错\n");
    exit(1);
} catch (\RuntimeException $e) {
    if (! str_contains($e->getMessage(), 'swow')) {
        fwrite(STDERR, "FAIL: 期望「swow extension is required」类提示，实际: {$e->getMessage()}\n");
        exit(1);
    }
    echo "✓ 缺 swow 时给出有效错误提示: {$e->getMessage()}" . PHP_EOL;
}

echo PHP_EOL . "★ SwowClient 结构性自检 PASS" . PHP_EOL;
