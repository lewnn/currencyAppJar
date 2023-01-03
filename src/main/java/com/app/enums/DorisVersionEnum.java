package com.app.enums;

public enum DorisVersionEnum {
    V1_1_1(001_001_001),
    V1_1_2(001_001_002),
    V1_0_0(001_000_000),
    V0_15_0(000_015_000);

    int version;

    DorisVersionEnum(int version) {
        this.version = version;
    }

    public int getVersion(){
        return this.version;
    }
}
