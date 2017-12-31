package com.lzy.common.constants;

/**
 * Created by Liu Zi Yang on 2017/6/18 18:22.
 * E-mail address is kobeliuziyang@qq.com
 * Copyright © 2017 Liuziyang. All Rights Reserved.
 *
 * @author Liuziyang
 */
public interface TableConstants {

    String TABLE_USER = "TABLE_USER";
    String TABLE_USER_VISIT = "TABLE_USER_VISIT";
    String TABLE_USER_VISIT_TIME = "TABLE_USER_VISIT_TIME";

    // table user_visit
    String FIELD_SHOP_ID = "shop_id";
    String FILED_MMAC = "mmac";
    String FIELD_TIME = "time";
    String FIELD_TOTAL_FLOW = "total_flow";
    String FIELD_CHECK_IN_FLOW = "check_in_flow";
    String FIELD_CHECK_IN_TATE = "check_in_rate";
    String FIELD_SHALLOW_VISIT_RATE = "shallow_visit_rate";
    String FIELD_DEEP_VISIT_RATE = "deep_visit_rate";

    //table user
    String FIELD_MAC = "mac";
    String FIELD_BRAND = "brand";
}
