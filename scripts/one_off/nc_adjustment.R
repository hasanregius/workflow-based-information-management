# NC Correction
# Y = 1 + 0.947328(X-1) + 0.009079(X-1)2

# Add the scatterplot + final model line
ortho = read.csv("~/Desktop/nc_ortho_tested_dilutional.csv") %>%
  filter(between(nc_sco_1,1,100))
fit = lm(I(nc_sco_20-50) ~ I(nc_sco_1-50) - 1, ortho)
ortho$calc = (50 + 1.5*((nc_sco_dil_1)-50) + (0.005441*(((nc_sco_dil_1)-50)^2)))*20

df = data.frame(
  x = seq(50,100,0.1),
  y = 50 + 1.5*(seq(50,100,0.1)-50) + 0.005441*((seq(50,100,0.1)-50)^2),
  z = 50 + 1.283521*(seq(50,100,0.1)-50) + 0.005441*((seq(50,100,0.1)-50)^2),
  a = 50 + 4.10401*(seq(50,100,0.1)-50) + -0.01897*((seq(50,100,0.1)-50)^2)
)

plot = ggplot(ortho, aes(x = nc_sco_1, y = nc_sco_20)) +
  geom_point() +
  geom_smooth(data = df, aes(x = x, y = y)) +
  geom_smooth(data = df, aes(x = x, y = z), color = "red") +
  geom_smooth(data = df, aes(x = x, y = a), color = "green") + theme_bw()
ggsave(plot = plot, filename = "~/Desktop/ortho_base_data_with_fitted_lines.tiff",
       width = 6, height = 4, dpi = 600)

# -1.5
ortho$b0 = 50
fit2 = glm(nc_sco_20 ~ - 1 + nc_sco_1 + I(nc_sco_1^2) + offset(b0),
  data =  ortho,
  family = gaussian(link = "identity"))
# Regression formula is Y = 50 + -1.50438794(X) + 0.03433374(X^2)
fit3 = glm(I(nc_sco_20-50) ~ 0 + I(nc_sco_1-50) + I((nc_sco_1-50)^2),
           data =  ortho,
           family = gaussian(link = 'identity'))

# Y = 50 + 4.10401(X-50) + -0.01897((X-50)^2)
# Y = 50 + 1.283521(X-50) + 0.005441((X-50)^2)
# Y = 50 + 1.5(X-50) + 0.005441((X-50)^2)
df = data.frame(nc_sco_1 = seq(50,100,0.5))
df$nc_sco_20_lin = predict.glm(fit, newdata = df)+50
df$nc_sco_20_qdc = 50 + 4.10401*(df$nc_sco_1-50) + (-0.01897*(df$nc_sco_1-50)^2)

nc_adjust_base = read.csv("~/Downloads/nc_dilutional_data_cts_pull_all_dins_20240405.csv")
nc_adjusted = nc_adjust_base %>%
  mutate(
    lin_nc_sco_dil_1 = case_when(
      nc_sco_dil_1 >= 50 ~ 1.05870*(nc_sco_dil_1-50)+50,
      nc_sco_dil_1 < 50 ~ nc_sco_dil_1
    ),
    qdc_nc_sco_dil_1 = case_when(
      nc_sco_dil_1 >= 50 ~ (50 + 1.5*(nc_sco_dil_1-50) + (0.005441*((nc_sco_dil_1-50)^2))),
      nc_sco_dil_1 < 50 ~ nc_sco_dil_1
    ),
    lin_nc_sco_dil_20 = case_when(
      (nc_sco_dil_20/20) >= 50 ~ 20*(1.05870*((nc_sco_dil_20/20)-50)+50),
      (nc_sco_dil_20/20) < 50 ~ nc_sco_dil_20
    ),
    qdc_nc_sco_dil_20 = case_when(
      (nc_sco_dil_20/20) >= 50 ~ (50 + 1.5*((nc_sco_dil_20/20)-50) + (0.005441*(((nc_sco_dil_20/20)-50)^2)))*20,
      (nc_sco_dil_20/20) < 50 ~ nc_sco_dil_20
    ),
    lin_nc_sco_dil_400 = case_when(
      (nc_sco_dil_400/400) >= 50 ~ 400*(1.05870*((nc_sco_dil_400/400)-50)+50),
      (nc_sco_dil_400/400) < 50 ~ nc_sco_dil_400
    ),
    qdc_nc_sco_dil_400 = case_when(
      (nc_sco_dil_400/400) >= 50 ~ (50 + 1.5*((nc_sco_dil_400/400)-50) + (0.005441*(((nc_sco_dil_400/400)-50)^2)))*400,
      (nc_sco_dil_400/400) < 50 ~ nc_sco_dil_400
    )
  )

nc_adjusted_base = nc_adjusted %>%
  select(din, nc_sco_dil_1, nc_sco_dil_20, nc_sco_dil_400) %>%
  pivot_longer(cols = c(nc_sco_dil_1, nc_sco_dil_20, nc_sco_dil_400),
               names_to = "dilution_factor", values_to = "nc_sco") %>%
  mutate(dilution_factor = case_when(
    dilution_factor == "nc_sco_dil_400" ~ 400,
    dilution_factor == "nc_sco_dil_20" ~ 20,
    dilution_factor == "nc_sco_dil_1" ~ 1
  )) %>%
  filter(between(nc_sco, 1, 9999999)) %>%
  group_by(din) %>%
  slice_max(dilution_factor, n = 1) %>%
  ungroup() %>%
  filter((dilution_factor == 1 & nc_sco >= 100) == F &
           (dilution_factor == 20 & nc_sco >= 2000) == F)


nc_corr = ggplot(nc_adjusted_base, aes(nc_sco, fill = as.factor(dilution_factor))) +
  geom_histogram(colour = "grey", bins = 75) + geom_density(stat = "density") +
  scale_x_log10(breaks = c(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000)) +
  geom_vline(xintercept = c(100, 2000)) + theme_bw() + ggtitle("Final NC SCO without correction")

ggsave(plot = nc_corr, width = 6, height = 4, filename = "~/Desktop/nc_dilutional_correction_base.tiff", dpi = 600)


nc_adjusted_linear = nc_adjusted %>%
  select(din, lin_nc_sco_dil_1, lin_nc_sco_dil_20, lin_nc_sco_dil_400) %>%
  pivot_longer(cols = c(lin_nc_sco_dil_1, lin_nc_sco_dil_20, lin_nc_sco_dil_400),
               names_to = "dilution_factor", values_to = "nc_sco") %>%
  mutate(dilution_factor = case_when(
    dilution_factor == "lin_nc_sco_dil_400" ~ 400,
    dilution_factor == "lin_nc_sco_dil_20" ~ 20,
    dilution_factor == "lin_nc_sco_dil_1" ~ 1
  )) %>%
  filter(nc_sco >= 1) %>%
  group_by(din) %>%
  slice_max(dilution_factor, n = 1)

lin_nc_corr = ggplot(nc_adjusted_linear, aes(nc_sco, fill = as.factor(dilution_factor))) +
  geom_histogram(colour = "grey", bins = 75) + geom_density(stat = "density") +
  scale_x_log10(breaks = c(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000)) +
  geom_vline(xintercept = c(100, 2000)) + theme_bw() + ggtitle("Final NC SCO with linear correction")

ggsave(plot = lin_nc_corr, width = 6, height = 4, filename = "~/Desktop/nc_dilutional_correction_linear.tiff", dpi = 600)

# NC adjusted exponential
nc_adjusted_exp = nc_adjusted %>%
  select(din, nc_sco_dil_1, exp_nc_sco_dil_20, exp_nc_sco_dil_400) %>%
  pivot_longer(cols = c(nc_sco_dil_1, exp_nc_sco_dil_20, exp_nc_sco_dil_400),
               names_to = "dilution_factor", values_to = "nc_sco") %>%
  mutate(dilution_factor = case_when(
    dilution_factor == "exp_nc_sco_dil_400" ~ 400,
    dilution_factor == "exp_nc_sco_dil_20" ~ 20,
    dilution_factor == "nc_sco_dil_1" ~ 1
  )) %>%
  filter(nc_sco >= 1) %>%
  group_by(din) %>%
  slice_max(dilution_factor, n = 1)

exp_nc_corr = ggplot(nc_adjusted_exp, aes(nc_sco, fill = as.factor(dilution_factor))) +
  geom_histogram(colour = "grey", bins = 75) + geom_density(stat = "density") +
  scale_x_log10(breaks = c(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000)) +
  geom_vline(xintercept = c(100, 2000)) + theme_bw() + ggtitle("Final NC SCO with exponential correction")

ggsave(plot = exp_nc_corr, width = 6, height = 4, filename = "~/Desktop/nc_dilutional_correction_exponential.tiff", dpi = 600)

nc_adjusted = nc_adjusted %>%
  mutate(
    final_not_adjusted = case_when(
      !is.na(nc_sco_dil_400) ~ nc_sco_dil_400,
      is.na(nc_sco_dil_400) & !is.na(nc_sco_dil_20) ~ nc_sco_dil_20,
      is.na(nc_sco_dil_400) & is.na(nc_sco_dil_20) & !is.na(nc_sco_dil_1) ~ nc_sco_dil_1),
    final_quadratic_adjusted = case_when(
      !is.na(qdc_nc_sco_dil_400) ~ qdc_nc_sco_dil_400,
      is.na(qdc_nc_sco_dil_400) & !is.na(qdc_nc_sco_dil_20) ~ qdc_nc_sco_dil_20,
      is.na(qdc_nc_sco_dil_400) & is.na(qdc_nc_sco_dil_20) & !is.na(qdc_nc_sco_dil_1) ~ qdc_nc_sco_dil_1)
  )

# NC adjusted exponential
nc_adjusted_qdc = nc_adjusted %>%
  select(din, qdc_nc_sco_dil_1, qdc_nc_sco_dil_20, qdc_nc_sco_dil_400) %>%
  pivot_longer(cols = c(qdc_nc_sco_dil_1, qdc_nc_sco_dil_20, qdc_nc_sco_dil_400),
               names_to = "dilution_factor", values_to = "nc_sco") %>%
  mutate(dilution_factor = case_when(
    dilution_factor == "qdc_nc_sco_dil_400" ~ 400,
    dilution_factor == "qdc_nc_sco_dil_20" ~ 20,
    dilution_factor == "qdc_nc_sco_dil_1" ~ 1
  )) %>%
  filter(nc_sco >= 1) %>%
  group_by(din) %>%
  slice_max(dilution_factor, n = 1)

qdc_nc_corr = ggplot(nc_adjusted_qdc, aes(nc_sco, fill = as.factor(dilution_factor))) +
  geom_histogram(colour = "grey", bins = 75) + geom_density(stat = "density") +
  scale_x_log10(breaks = c(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000)) +
  geom_vline(xintercept = c(100, 2000)) + theme_bw() + ggtitle("Final NC SCO with quadratic correction (m = 1.5)")
ggsave(plot = qdc_nc_corr, width = 6, height = 4, filename = "~/Desktop/nc_dilutional_correction_quadratic_m_1,5.tiff", dpi = 600)

nc_show_linear = nc_adjust_base %>% filter(nc_sco_dil_1 < 100 &)
fit = lm(nc_sco_dil_20 ~ nc_sco_dil_1 ,data = nc_show_linear)
ggplot(nc_show_linear, aes(nc_sco_dil_1, nc_sco_dil_20)) +
  geom_point()

summary(fit)

adjusted_nc = nc_results_db %>%
  mutate(adjusted_nc_sco = case_when(
    nc_total_ig_test == "ortho" & nc_sco >= 1 ~ 1 + nc_dilution_factor*(0.947328*((nc_sco/nc_dilution_factor) - 1) + nc_dilution_factor*(0.009079*((nc_sco/nc_dilution_factor) - 1))*2)
  ))

ggplot(nc_results_db, aes(nc_sco, fill = as.factor(nc_dilution_factor))) +
  geom_histogram(colour = "grey", bins = 75) + geom_density(stat = "density") +
  scale_x_log10(breaks = c(1, 10, 100, 1000, 10000, 100000, 1000000, 10000000)) +
  geom_vline(xintercept = c(100, 2000)) + theme_bw() + ggtitle("Final NC SCO pre-adjustment")


mispa_need_igg = mispa %>%
  filter(is.na(vitros_quant_s_igg_bauml) & study_name == "CDC Repeat Donor Cohort") %>%
  select(donor_id, `freezerworks id`, din)

write.csv(mispa_need_igg, "~/Desktop/mispa_panel_need_igg.csv", row.names = F)


