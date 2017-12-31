package com.lzy.service.impl;

import com.lzy.entity.ProbeUser;
import com.lzy.entity.User;
import com.lzy.service.ProbeUserService;
import com.lzy.mapper.ProbeUserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by maicius on 2017/6/28.
 */
@Service
public class ProbeUserServiceImpl implements ProbeUserService {

    @Autowired
    private ProbeUserMapper probeUserMapper;

    public List<ProbeUser> queryProbeUser(User user) throws Exception {
        return probeUserMapper.queryProbeUser(user);
    }

    public void setProbeUser(List<ProbeUser> probeUser) throws Exception {
        probeUserMapper.setProbeUser(probeUser);
    }
}
