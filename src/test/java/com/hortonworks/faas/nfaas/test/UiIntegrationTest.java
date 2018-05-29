package com.hortonworks.faas.nfaas.test;

import com.hortonworks.faas.nfaas.app.HdfFlowMigratorApp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = HdfFlowMigratorApp.class, webEnvironment = WebEnvironment.RANDOM_PORT)
public class UiIntegrationTest {

    @Test
    public void whenLoadApplication_thenSuccess() {

    }
}
