module github.com/vesoft-inc/nebula-br

go 1.16

replace github.com/vesoft-inc/nebula-agent => /home/pengwei.song/code/nebula-agent

require (
	github.com/facebook/fbthrift v0.31.1-0.20211129061412-801ed7f9f295
	github.com/google/uuid v1.3.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.0
	github.com/vesoft-inc/nebula-agent v0.0.0-20211026035604-1fdf4f481d68
	github.com/vesoft-inc/nebula-go/v2 v2.5.2-0.20211228055601-b5b11a36e453
	golang.org/x/sys v0.0.0-20211124211545-fe61309f8881 // indirect
)
