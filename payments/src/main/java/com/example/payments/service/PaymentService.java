package com.example.payments.service;

import com.example.payments.DTO.Order;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {

    public boolean createPayment(Order order) {
        return order != null;
    }
}
