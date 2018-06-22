package com.hortonworks.faas.nfaas.controller;

import com.hortonworks.faas.nfaas.dto.Address;
import com.hortonworks.faas.nfaas.dto.Customer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;



@RestController
public class RestAPIs {

    Map<Long, Customer> custStores = new HashMap<Long, Customer>();

    @PostConstruct
    public void initIt() throws Exception {
        custStores.put(Long.valueOf(1), new Customer(new Long(1), "Jack", 25, new Address("NANTERRE CT", "77471")));
        custStores.put(Long.valueOf(2), new Customer(new Long(2), "Mary", 37, new Address("W NORMA ST", "77009")));
        custStores.put(Long.valueOf(3), new Customer(new Long(3), "Peter", 18, new Address("S NUGENT AVE", "77571")));
        custStores.put(Long.valueOf(4), new Customer(new Long(4), "Amos", 23, new Address("E NAVAHO TRL", "77449")));
        custStores.put(Long.valueOf(5), new Customer(new Long(5), "Craig", 45, new Address("AVE N", "77587")));
    }

    @GetMapping(value = "/allcustomeradrress")
    public List<Customer> getResource() {

        List<Customer> custList = custStores.entrySet().stream()
                .map(entry -> entry.getValue())
                .collect(Collectors.toList());

        return custList;
    }
}