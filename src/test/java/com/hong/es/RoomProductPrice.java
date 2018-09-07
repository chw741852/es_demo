package com.hong.es;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "t_room_product_price", type = "doc")
public class RoomProductPrice {
    @Id
    private Long id;

    private String createtime;

    private Integer productid;

    /**
     * 开始时间，格式：yyyy-MM-dd
     */
    private String startdate;

    /**
     * 结束时间，格式：yyyy-MM-dd
     */
    private String enddate;

    /**
     * 底价
     */
    private Integer costprice;

    /**
     * 可预订套数
     */
    private Integer stock;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCreatetime() {
        return createtime;
    }

    public void setCreatetime(String createtime) {
        this.createtime = createtime;
    }

    public Integer getProductid() {
        return productid;
    }

    public void setProductid(Integer productid) {
        this.productid = productid;
    }

    public String getStartdate() {
        return startdate;
    }

    public void setStartdate(String startdate) {
        this.startdate = startdate;
    }

    public String getEnddate() {
        return enddate;
    }

    public void setEnddate(String enddate) {
        this.enddate = enddate;
    }

    public Integer getCostprice() {
        return costprice;
    }

    public void setCostprice(Integer costprice) {
        this.costprice = costprice;
    }

    public Integer getStock() {
        return stock;
    }

    public void setStock(Integer stock) {
        this.stock = stock;
    }
}