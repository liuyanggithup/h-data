package beibei.vo;


public class AreaVo {

    private String beijing;
    private String shanghai;
    private String guangzhou;
    private String shenzhen;
    private String chengdu;

    public String getBeijing() {
        return beijing;
    }

    public void setBeijing(String beijing) {
        this.beijing = beijing;
    }

    public String getShanghai() {
        return shanghai;
    }

    public void setShanghai(String shanghai) {
        this.shanghai = shanghai;
    }

    public String getGuangzhou() {
        return guangzhou;
    }

    public void setGuangzhou(String guangzhou) {
        this.guangzhou = guangzhou;
    }

    public String getShenzhen() {
        return shenzhen;
    }

    public void setShenzhen(String shenzhen) {
        this.shenzhen = shenzhen;
    }

    public String getChengdu() {
        return chengdu;
    }

    public void setChengdu(String chengdu) {
        this.chengdu = chengdu;
    }

    public void setData(String areaid, String amt) {
        if (areaid.equals("1")) {
            this.setBeijing(amt);
        } else if (areaid.equals("2")) {
            this.setShanghai(amt);
        } else if (areaid.equals("3")) {
            this.setGuangzhou(amt);
        } else if (areaid.equals("4")) {
            this.setShenzhen(amt);
        } else if (areaid.equals("5")) {
            this.setChengdu(amt);
        }
    }


}
