package org.sunbird.workflow.postgres.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "wf_domain_lookup", schema = "wingspan")
public class WfDomainLookup extends Object{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "wf_domain_id", nullable = false)
    private int id;

    @Column(name = "wf_id")
    private String wfId;

    @Column(name = "domain_name")
    private String domainName;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getWfId() {
        return wfId;
    }

    public void setWfId(String wfId) {
        this.wfId = wfId;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }
}
