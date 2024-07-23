

x1 <- haven::read_xpt("data/something2.xpt")
x2 <- haven::read_xpt("data/performance.xpt")


ncol(x1)
ncol(x2)

all.equal(x1, x2)



diffdf::diffdf(x1, x2)


dput(attributes(x1))
dput(attributes(x2))

