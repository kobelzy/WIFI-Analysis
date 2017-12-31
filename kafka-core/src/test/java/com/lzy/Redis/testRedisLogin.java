package com.lzy.Redis;

import com.lzy.BaseResources;
import com.lzy.entity.User;
import com.lzy.service.impl.LoginServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by maicius on 2017/7/25.
 */

@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration( classes = {LoginService.class, LoginServiceImpl.class, LoginDAO.class})
public class testRedisLogin extends BaseResources{
//    @Autowired
//    private LoginService loginService;
    private LoginServiceImpl loginService = new LoginServiceImpl();
    @Test
    public void testRedisLogin() throws Exception{
        User user = new User();
        user.setUserName("18996720676");
        String res = loginService.verifyCode(user);
        if(res.length() > 0){
            user.setVerifyCode(res);
            user.setPassword(res);
            User newUser = loginService.doUserLogin(user);
            //登陆成功返回用户名
            System.out.println(newUser.getUserName());
        }
    }
}
