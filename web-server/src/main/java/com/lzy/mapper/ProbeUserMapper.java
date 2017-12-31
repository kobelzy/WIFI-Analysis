package com.lzy.mapper;

import com.lzy.entity.ProbeUser;
import com.lzy.entity.User;

import java.util.List;

/**
 * Created by maicius on 2017/6/28.
 */
public interface ProbeUserMapper {
    List<ProbeUser> queryProbeUser(User user) throws Exception;
    void setProbeUser(List<ProbeUser> probeUser) throws Exception;
}
