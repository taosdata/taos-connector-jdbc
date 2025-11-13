package com.taosdata.jdbc.ws.loadbalance;

public enum StepEnum {
    CONNECT(0),
    CON_CMD(1),
    QUERTY(2),
    FETCH(3),
    FETCH2(4),
    FINISH(5),

    ;
    private final int step;

    StepEnum(int step) {
        this.step = step;
    }

    public int get() {
        return step;
    }

    /**
     * 根据 int 值获取对应的枚举字符串名称（如 0 → "CONNECT"）
     * @param step 步骤的 int 值
     * @return 对应的枚举名称（字符串）
     * @throws IllegalArgumentException 当 int 值无匹配的枚举时抛出
     */
    public static String getNameByInt(int step) {
        for (StepEnum stepEnum : values()) {
            if (stepEnum.step == step) {
                return stepEnum.name(); // 返回枚举的字符串名称（如 CONNECT、CON_CMD）
            }
        }
        throw new IllegalArgumentException("No matching StepEnum for int value: " + step);
    }
}
