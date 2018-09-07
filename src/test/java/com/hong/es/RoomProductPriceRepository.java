package com.hong.es;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface RoomProductPriceRepository extends ElasticsearchRepository<RoomProductPrice, Long> {
    List<RoomProductPrice> findByStartdateBetween(String s, String e);
    List<RoomProductPrice> findByProductid(Long productId);
    RoomProductPrice getById(Long id);
}
