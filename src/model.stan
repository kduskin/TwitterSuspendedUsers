data {
  int<lower=1> N;
  array[N] real x;
  array[N] real banned;
  array[N] int y;
  array[N] int step; 
}
transformed data {
  real delta = 1e-9;
}
parameters {
  real<lower=0> rho;
  real<lower=0> alpha;
  real<lower=0> phi;
  real mu0;
  real mu1;

  real b0;
  real b2;

  vector[N] eta;
}
transformed parameters {
vector[N] f;
{
matrix[N, N] L_K;
matrix[N, N] K = gp_exp_quad_cov(step, alpha, rho);

// diagonal elements
for (n in 1:N) {
  K[n, n] = K[n, n] + delta;
}

L_K = cholesky_decompose(K);
f = L_K * eta;
}


}
model {
  vector[N] mu;

  rho ~ inv_gamma(5, 5);
  alpha ~ std_normal();
  phi ~ std_normal();
  eta ~ std_normal();

  mu0 ~ normal(0,5);
  mu1 ~ normal(0,5);

  b0 ~ normal(0,2);
  b2 ~ normal(0,2);

  for(n in 1:N){
    mu[n] = f[n] + mu0 + b0*x[n] + mu1*(banned[n])+ b2*(banned[n])*x[n];

  }
  y ~ neg_binomial_2_log(mu, phi);
}

generated quantities {
  vector[N] y_hat;
  vector[N] mu_hat;
  vector[N] exp_hat;
  vector[N] log_lik;

  vector[N] mu_hat_without_ban;
  vector[N] exp_hat_without_ban;
  vector[N] y_without_ban;



  for (n in 1:N) {
    mu_hat[n] =   b0*x[n] + mu0 + mu1*(banned[n])+ b2*(banned[n])*x[n];
    exp_hat[n] = f[n] + mu_hat[n];
    y_hat[n] = neg_binomial_2_log_rng(exp_hat[n], phi);
    log_lik[n] = neg_binomial_2_log_lpmf(y[n] | exp_hat[n], phi);

    mu_hat_without_ban[n] =  b0*x[n] + mu0;
    exp_hat_without_ban[n] = f[n] + mu_hat_without_ban[n];
    y_without_ban[n] = neg_binomial_2_log_rng(exp_hat_without_ban[n], phi);






  }
}
