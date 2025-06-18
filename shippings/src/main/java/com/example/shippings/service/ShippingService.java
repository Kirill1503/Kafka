package com.example.shippings.service;

import com.example.shippings.DTO.Order;
import org.springframework.stereotype.Service;

@Service
public class ShippingService {

    public boolean shippingCreated(Order order) {
        return !order.getName().isEmpty();
    }
}