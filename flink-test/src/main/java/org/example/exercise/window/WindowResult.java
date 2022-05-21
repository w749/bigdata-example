package org.example.exercise.window;

import java.sql.Timestamp;

/**
 * 包装输出结果
 */
public class WindowResult {
    private String path;
    private int uv;
    private long start;
    private long end;

    public WindowResult(String path, int uv, long start, long end) {
        this.path = path;
        this.uv = uv;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
        return "WindowResult{" +
                "path='" + path + '\'' +
                ", uv=" + uv +
                ", start=" + new Timestamp(start) +
                ", end=" + new Timestamp(end) +
                '}';
    }
}
