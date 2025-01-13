package org.sunbird.workflow.postgres.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "wf_domain_user_info", schema = "wingspan")
public class WfDomainUserInfo extends Object {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "wf_domain_user_info_id", nullable = false)
    private int id;

    @Column(name = "domain_name")
    private String domainName;

    @Column(name = "first_name")
    private String firstName;

    @Column(name = "email")
    private String email;

    @Column(name = "mobile")
    private String mobile;

    @Column(name = "description")
    private String description;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
